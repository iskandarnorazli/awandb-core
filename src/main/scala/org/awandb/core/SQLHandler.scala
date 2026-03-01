/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.awandb.core.sql

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.{
  Select,
  PlainSelect,
  SelectItem,
  OrderByElement,
  Limit
}
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.update.Update
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.drop.Drop
import net.sf.jsqlparser.statement.alter.Alter
import net.sf.jsqlparser.expression.operators.relational.{
  EqualsTo,
  GreaterThan,
  GreaterThanEquals,
  MinorThan,
  MinorThanEquals,
  InExpression,
  LikeExpression,
  IsNullExpression
}
import net.sf.jsqlparser.expression.operators.conditional.{
  AndExpression,
  OrExpression
}
import net.sf.jsqlparser.expression.{
  LongValue,
  StringValue,
  Function,
  Expression,
  Parenthesis
}
import net.sf.jsqlparser.schema.{Column, Table}
import org.awandb.core.engine.AwanTable
import org.awandb.core.query.{
  HashJoinBuildOperator,
  HashJoinProbeOperator,
  TableScanOperator
}
import org.awandb.core.jni.NativeBridge
import scala.jdk.CollectionConverters._

// Structured Result Class
case class SQLResult(isError: Boolean, message: String, affectedRows: Long = 0L)

object SQLHandler {

  val tables = new java.util.concurrent.ConcurrentHashMap[String, AwanTable]()

  def register(name: String, table: AwanTable): Unit = {
    tables.put(name.toLowerCase, table)
  }

  def execute(sql: String): SQLResult ={
    val hasReturning = sql.toUpperCase().contains(" RETURNING")
    try {
      val statement = CCJSqlParserUtil.parse(sql)

      // Global Helper to strip table prefixes for internal array lookups
      def cleanColName(rawName: String): String = {
          val lower = rawName.toLowerCase
          if (lower.contains(".")) lower.split("\\.").last else lower
      }

      // Shared WHERE evaluator for Unrestricted DML
          def evaluateWhere(expr: Expression, table: AwanTable): Array[Int] = {
              
              // Helper to safely extract (ColumnName, TargetValue) and prevent ClassCastExceptions
              def getColAndVal(op: net.sf.jsqlparser.expression.operators.relational.ComparisonOperator): (String, Int) = {
                 if (!op.getLeftExpression.isInstanceOf[Column]) {
                    throw new RuntimeException("Left side of condition must be a column name.")
                 }
                 if (!op.getRightExpression.isInstanceOf[LongValue]) {
                    throw new RuntimeException("Right side of condition must be an integer.")
                 }
                 
                 val rawName = op.getLeftExpression.asInstanceOf[Column].getFullyQualifiedName
                 val colName = cleanColName(rawName)
                 
                 if (!table.columnOrder.contains(colName)) {
                     throw new RuntimeException(s"Column '$colName' not found.")
                 }
                 (colName, op.getRightExpression.asInstanceOf[LongValue].getValue.toInt)
              }

              expr match {
                 case p: Parenthesis => evaluateWhere(p.getExpression, table)
                 case and: AndExpression =>
                    val leftIds = evaluateWhere(and.getLeftExpression, table)
                    val rightIdsSet = evaluateWhere(and.getRightExpression, table).toSet
                    leftIds.filter(rightIdsSet.contains)
                 case or: OrExpression =>
                    val leftIds = evaluateWhere(or.getLeftExpression, table)
                    val rightIds = evaluateWhere(or.getRightExpression, table)
                    (leftIds.toSet ++ rightIds.toSet).toArray
                    
                 case eq: EqualsTo =>
                    val left = eq.getLeftExpression
                    // [NEW] VECTOR SEARCH INTERCEPTOR
                    if (left.isInstanceOf[Function] && left.asInstanceOf[Function].getName.equalsIgnoreCase("VECTOR_SEARCH")) {
                       val func = left.asInstanceOf[Function]
                       val params = func.getParameters.getExpressions
                       
                       val rawCol = params.get(0) match {
                         case c: Column => c.getFullyQualifiedName
                         case _ => params.get(0).toString
                       }
                       val colName = cleanColName(rawCol)
                       
                       // [CRITICAL FIX] Explicitly check column existence and throw SQL error
                       if (!table.columnOrder.contains(colName)) {
                           throw new RuntimeException(s"Column '$colName' not found.")
                       }
                       
                       val vecStr = params.get(1).asInstanceOf[StringValue].getValue
                       val threshold = params.get(2) match {
                          case d: net.sf.jsqlparser.expression.DoubleValue => d.getValue.toFloat
                          case l: net.sf.jsqlparser.expression.LongValue => l.getValue.toFloat
                          case _ => 0.0f
                       }
                       val vecArray = vecStr.stripPrefix("[").stripSuffix("]").split(",").map(_.trim.toFloat)
                       return table.queryVector(colName, vecArray, threshold)
                    }

                    val (colName, targetVal) = getColAndVal(eq)
                    table.scanFilteredIds(colName, 0, targetVal)
                 case gt: GreaterThan =>
                    val (colName, targetVal) = getColAndVal(gt)
                    table.scanFilteredIds(colName, 1, targetVal)
                 case gte: GreaterThanEquals =>
                    val (colName, targetVal) = getColAndVal(gte)
                    table.scanFilteredIds(colName, 2, targetVal)
                 case lt: MinorThan =>
                    val (colName, targetVal) = getColAndVal(lt)
                    table.scanFilteredIds(colName, 3, targetVal)
                 case lte: MinorThanEquals =>
                    val (colName, targetVal) = getColAndVal(lte)
                    table.scanFilteredIds(colName, 4, targetVal)

                 case in: InExpression =>
                    val rawName = in.getLeftExpression.asInstanceOf[Column].getFullyQualifiedName
                    val colName = cleanColName(rawName)
                    if (!table.columnOrder.contains(colName)) throw new RuntimeException(s"Column '$colName' not found.")
                    
                    val inStr = in.toString 
                    val content = inStr.substring(inStr.indexOf('(') + 1, inStr.lastIndexOf(')'))
                    val targetVals = content.split(",").map(_.trim.replace("'", "")).toSet

                    val allIds = table.scanFilteredIds(table.columnOrder.head, 2, 0)
                    val colIdx = table.columnOrder.indexOf(colName)
                    
                    allIds.filter { id =>
                       val rowOpt = table.getRow(id)
                       rowOpt.isDefined && targetVals.contains(rowOpt.get(colIdx).toString)
                    }

                 case like: LikeExpression =>
                    val rawName = like.getLeftExpression.asInstanceOf[Column].getFullyQualifiedName
                    val colName = cleanColName(rawName)
                    if (!table.columnOrder.contains(colName)) throw new RuntimeException(s"Column '$colName' not found.")
                    
                    val targetVal = like.getRightExpression.asInstanceOf[StringValue].getValue
                    val regexStr = "^" + targetVal.replace("%", ".*").replace("_", ".") + "$"
                    val regex = regexStr.r
                    
                    val allIds = table.scanFilteredIds(table.columnOrder.head, 2, 0)
                    val colIdx = table.columnOrder.indexOf(colName)
                    
                    allIds.filter { id =>
                       val rowOpt = table.getRow(id)
                       rowOpt.isDefined && regex.matches(rowOpt.get(colIdx).toString)
                    }

                 case isNull: IsNullExpression =>
                    val rawName = isNull.getLeftExpression.asInstanceOf[Column].getFullyQualifiedName
                    val colName = cleanColName(rawName)
                    if (!table.columnOrder.contains(colName)) throw new RuntimeException(s"Column '$colName' not found.")
                    
                    val isNot = isNull.isNot
                    val allIds = table.scanFilteredIds(table.columnOrder.head, 2, 0)
                    val colIdx = table.columnOrder.indexOf(colName)
                    
                    allIds.filter { id =>
                       val rowOpt = table.getRow(id)
                       if (rowOpt.isDefined) {
                           val v = rowOpt.get(colIdx)
                           val isNullEquivalent = v == null || v == "" || v == 0
                           if (isNot) !isNullEquivalent else isNullEquivalent
                       } else false
                    }
                    
                 case _ => throw new RuntimeException(s"Unsupported WHERE clause operator: ${expr.getClass.getSimpleName}")
              }
          }

      statement match {
        case select: Select =>
          val plain = select.getSelectBody.asInstanceOf[PlainSelect]
          val leftTableName = plain.getFromItem.asInstanceOf[Table].getName.toLowerCase
          val leftTable = tables.get(leftTableName)
          if (leftTable == null) return SQLResult(true, s"Error: Table '$leftTableName' not found.")

          // 1. CHECK FOR GROUP BY
          val groupBy = plain.getGroupBy
          if (groupBy != null) {
            val rawKey = groupBy.getGroupByExpressionList.getExpressions.get(0).asInstanceOf[Column].getFullyQualifiedName
            val keyCol = cleanColName(rawKey)
            
            var valCol = ""
            var rawValCol = ""
            for (item <- plain.getSelectItems.asScala) {
              val expr = item.getExpression
              if (expr != null && expr.isInstanceOf[Function]) {
                val func = expr.asInstanceOf[Function]
                if (func.getName.equalsIgnoreCase("sum")) {
                  rawValCol = func.getParameters.getExpressions.get(0).asInstanceOf[Column].getFullyQualifiedName
                  valCol = cleanColName(rawValCol)
                }
              }
            }
            if (valCol.isEmpty)
              return SQLResult(true, "Error: GROUP BY currently requires a SUM(column) aggregate.")
            val resultMap = leftTable.executeGroupBy(keyCol, valCol)
            if (resultMap.isEmpty) return SQLResult(false, "   GROUP BY Results: (Empty)", 0L)

            val sb = new StringBuilder()
            sb.append(s"\n   GROUP BY Results ($rawKey | SUM($rawValCol)):\n")
            resultMap.foreach { case (k, v) => sb.append(s"   $k | $v\n") }
            return SQLResult(false, sb.toString(), resultMap.size.toLong)
          }

          // 1.5. CHECK FOR GRAPH BFS
          var isGraphBfs = false
          var bfsStartNode = 0
          for (item <- plain.getSelectItems.asScala) {
            val expr = item.getExpression
            if (expr != null && expr.isInstanceOf[Function]) {
              val func = expr.asInstanceOf[Function]
              if (func.getName.equalsIgnoreCase("BFS_DISTANCE")) {
                isGraphBfs = true
                val params = func.getParameters.getExpressions
                bfsStartNode = params.get(0).asInstanceOf[LongValue].getValue.toInt
              }
            }
          }

          // [NEW] GRAPH ENGINE INTERCEPTOR
          if (isGraphBfs) {
            try {
                // Assumes columns are standard 'src_id' and 'dst_id'. 
                // Could be extended to read arguments if needed.
                val graph = leftTable.projectToGraph("src_id", "dst_id")
                val distances = graph.bfs(bfsStartNode)
                
                val sb = new StringBuilder()
                sb.append(s"\n   BFS Distances from Node $bfsStartNode:\n")
                sb.append("   Node ID | Distance\n")
                
                var count = 0L
                for (i <- distances.indices) {
                   if (distances(i) != -1) {
                      sb.append(s"   $i | ${distances(i)}\n")
                      count += 1
                   }
                }
                graph.close()
                return SQLResult(false, sb.toString(), count)
            } catch {
                case e: Exception => return SQLResult(true, s"Graph Error: ${e.getMessage}")
            }
          }

          // 2. CHECK FOR JOIN
          val joins = plain.getJoins
          if (joins != null && !joins.isEmpty) {
            val join = joins.get(0)
            val rightTableName = join.getRightItem.asInstanceOf[Table].getName.toLowerCase
            val rightTable = tables.get(rightTableName)
            if (rightTable == null) return SQLResult(true, s"Error: Join Table '$rightTableName' not found.")

            val onExpr = join.getOnExpressions.iterator().next().asInstanceOf[EqualsTo]
            val leftJoinCol = cleanColName(onExpr.getLeftExpression.asInstanceOf[Column].getFullyQualifiedName)
            val rightJoinCol = cleanColName(onExpr.getRightExpression.asInstanceOf[Column].getFullyQualifiedName)

            rightTable.flush()

            val leftKeyIdx = leftTable.columnOrder.indexOf(leftJoinCol)
            val leftPayloadIdx = if (leftTable.columnOrder.length > 2) 2 else 0
            val rightKeyIdx = rightTable.columnOrder.indexOf(rightJoinCol)
            val rightPayloadIdx = if (rightTable.columnOrder.length > 1) 1 else 0

            var totalMatches = 0
            val sb = new StringBuilder()
            sb.append(s"\n   JOIN Results (Probe Output):\n")

            // --- 1. BUILD PHASE (Right Table - Disk Only) ---
            val rightScan = new TableScanOperator(rightTable.blockManager, rightTable.blockManager.getLoadedBlocks.toArray, rightKeyIdx, rightPayloadIdx)
            val buildOp = new HashJoinBuildOperator(rightScan)
            buildOp.open() 

            // --- 2. PROBE PHASE A (Left Table - Disk) ---
            val leftScan = new TableScanOperator(leftTable.blockManager, leftTable.blockManager.getLoadedBlocks.toArray, leftKeyIdx, leftPayloadIdx)
            val probeOp = new HashJoinProbeOperator(leftScan, buildOp)
            probeOp.open()
            
            var batch = probeOp.next()
            while (batch != null && batch.count > 0) {
              totalMatches += batch.count
              val keys = new Array[Int](batch.count)
              val payloads = new Array[Long](batch.count)
              NativeBridge.copyToScala(batch.keysPtr, keys, batch.count)
              NativeBridge.copyToScalaLong(batch.valuesPtr, payloads, batch.count)
              for (i <- 0 until math.min(batch.count, 3)) { 
                sb.append(s"   [Disk] Match -> LeftKey: ${keys(i)} | RightPayload: ${payloads(i)}\n")
              }
              batch = probeOp.next()
            }
            probeOp.close()

            // --- 3. PROBE PHASE B (Left Table - RAM) ---
            val ramKeysBuffer = leftTable.columns(leftJoinCol).deltaIntBuffer
            val validRamKeys = new scala.collection.mutable.ArrayBuffer[Int]()
            
            for (i <- 0 until ramKeysBuffer.length) {
                if (!leftTable.ramDeleted.get(i)) {
                    validRamKeys.append(ramKeysBuffer(i))
                }
            }
            
            if (validRamKeys.nonEmpty) {
                val probeKeysPtr = NativeBridge.allocMainStore(validRamKeys.length)
                val outPayloadsPtr = NativeBridge.allocMainStore(validRamKeys.length * 2)
                val outIndicesPtr = NativeBridge.allocMainStore(validRamKeys.length)
                
                NativeBridge.loadData(probeKeysPtr, validRamKeys.toArray)
                
                val ramMatches = NativeBridge.joinProbe(buildOp.mapPtr, probeKeysPtr, validRamKeys.length, outPayloadsPtr, outIndicesPtr)
                totalMatches += ramMatches
                
                if (ramMatches > 0) {
                    val matchedPayloads = new Array[Long](ramMatches)
                    NativeBridge.copyToScalaLong(outPayloadsPtr, matchedPayloads, ramMatches)
                    for (i <- 0 until math.min(ramMatches, 2)) {
                       sb.append(s"   [RAM] Match -> RightPayload: ${matchedPayloads(i)}\n")
                    }
                }
                
                NativeBridge.freeMainStore(probeKeysPtr)
                NativeBridge.freeMainStore(outPayloadsPtr)
                NativeBridge.freeMainStore(outIndicesPtr)
            }
            
            buildOp.close() 

            sb.append(s"   Total Matches Found: $totalMatches\n")
            return SQLResult(false, sb.toString(), totalMatches.toLong)
          }

          // -------------------------------------------------------
          // 3. PIPELINE: FILTER -> SORT -> LIMIT -> PROJECT
          // -------------------------------------------------------

          val where = plain.getWhere
          var finalRows = if (where == null) {
              leftTable.scanFiltered(leftTable.columnOrder.head, 2, 0).toSeq
          } else {
              try { 
                  val matchedIds = evaluateWhere(where, leftTable)
                  matchedIds.flatMap(id => leftTable.getRow(id)).toSeq
              } 
              catch { case e: RuntimeException => return SQLResult(true, "Error: " + e.getMessage) }
          }

          // --- SORT (ORDER BY) ---
          val orderByElements = plain.getOrderByElements
          if (orderByElements != null && !orderByElements.isEmpty) {
            val orderBy = orderByElements.get(0)
            val rawSortName = orderBy.getExpression.asInstanceOf[Column].getFullyQualifiedName
            val sortColName = cleanColName(rawSortName)
            val sortColIdx = leftTable.columnOrder.indexOf(sortColName)

            if (sortColIdx == -1)
              return SQLResult(
                true,
                s"Error: ORDER BY column '$rawSortName' not found."
              )

            val isAsc = orderBy.isAsc
            finalRows = finalRows.sortWith { (r1, r2) =>
              val cmp = (r1(sortColIdx), r2(sortColIdx)) match {
                case (i1: Int, i2: Int)       => i1.compareTo(i2)
                case (l1: Long, l2: Long)     => l1.compareTo(l2)
                case (s1: String, s2: String) => s1.compareTo(s2)
                case _                        => 0
              }
              if (isAsc) cmp < 0 else cmp > 0
            }
          }

          // --- LIMIT ---
          val limit = plain.getLimit
          if (limit != null) {
            val limitExpr = limit.getRowCount
            if (limitExpr != null && limitExpr.isInstanceOf[LongValue]) {
              val limitVal = limitExpr.asInstanceOf[LongValue].getValue.toInt
              finalRows = finalRows.take(limitVal)
            }
          }
          
          // --- PROJECT & FORMAT OUTPUT ---
          val requestedColumns = scala.collection.mutable.ArrayBuffer[String]()
          val requestedHeaders = scala.collection.mutable.ArrayBuffer[String]()
          
          val aggregators = scala.collection.mutable.ArrayBuffer[(String, String)]()
          var isAggregation = false
          
          val isAsterisk = plain.getSelectItems.get(0).toString == "*"
          
          if (isAsterisk) {
             requestedColumns ++= leftTable.columnOrder
             requestedHeaders ++= leftTable.columnOrder
          } else {
             for (item <- plain.getSelectItems.asScala) {
                 val expr = item.getExpression
                 val alias = if (item.getAlias != null) item.getAlias.getName else null

                 if (expr != null && expr.isInstanceOf[Function]) {
                     isAggregation = true
                     val func = expr.asInstanceOf[Function]
                     val funcName = func.getName.toUpperCase
                     
                     val rawParam = if (func.isAllColumns) "*" else {
                         val params = func.getParameters
                         if (params != null && params.getExpressions != null && params.getExpressions.size() > 0) {
                             val firstExpr = params.getExpressions.get(0)
                             firstExpr match {
                                 case _: net.sf.jsqlparser.statement.select.AllColumns => "*"
                                 case c: Column => c.getFullyQualifiedName 
                                 case _ => firstExpr.toString 
                             }
                         } else "*"
                     }
                     val cleanParam = if (rawParam == "*") "*" else cleanColName(rawParam)
                     
                     requestedHeaders.append(if (alias != null) alias else s"$funcName($rawParam)")
                     aggregators.append((funcName, cleanParam))
                     
                 } else if (expr != null && expr.isInstanceOf[Column]) {
                     val c = expr.asInstanceOf[Column]
                     val rawColName = c.getFullyQualifiedName
                     val cleanName = cleanColName(rawColName)
                     
                     requestedColumns.append(cleanName)
                     requestedHeaders.append(if (alias != null) alias else rawColName)
                 }
             }
          }
          
          if (isAggregation) {
             var count = 0L
             val sums = Array.fill[Long](aggregators.size)(0L)
             val maxs = Array.fill[Int](aggregators.size)(Int.MinValue)
             val mins = Array.fill[Int](aggregators.size)(Int.MaxValue)
             
             for (row <- finalRows) {
                 count += 1
                 for (i <- aggregators.indices) {
                     val colName = aggregators(i)._2
                     val colIdx = leftTable.columnOrder.indexOf(colName)
                     if (colIdx != -1) {
                         val v = row(colIdx) match { case n: Int => n; case _ => 0 }
                         sums(i) += v
                         if (v > maxs(i)) maxs(i) = v
                         if (v < mins(i)) mins(i) = v
                     }
                 }
             }
             
             val finalAggRow = aggregators.indices.map { i =>
                 aggregators(i)._1 match {
                     case "COUNT" => count.toString
                     case "SUM" => if (count == 0) "NULL" else sums(i).toString
                     case "MAX" => if (count == 0) "NULL" else maxs(i).toString
                     case "MIN" => if (count == 0) "NULL" else mins(i).toString
                     case "AVG" => if (count == 0) "NULL" else (sums(i).toDouble / count).toString
                     case _ => "NULL"
                 }
             }
             
             val sb = new StringBuilder()
             sb.append(s"\n   Found Rows (${requestedHeaders.mkString(" | ")}):\n")
             sb.append("   " + finalAggRow.mkString(" | ") + "\n")
             return SQLResult(false, sb.toString(), 1L)
          }
          
          val projectionIndices = requestedColumns.map(col => leftTable.columnOrder.indexOf(col)).toArray
          if (projectionIndices.contains(-1)) return SQLResult(true, s"Error: One or more projected columns do not exist.")
          
          val sb = new StringBuilder()
          sb.append(s"\n   Found Rows (${requestedHeaders.mkString(" | ")}):\n")
          
          finalRows.foreach { row => 
             val projectedRow = projectionIndices.map(idx => row(idx))
             sb.append("   " + projectedRow.mkString(" | ") + "\n") 
          }
          return SQLResult(false, sb.toString(), finalRows.length.toLong)

        case insert: Insert =>
          val tableName = insert.getTable.getName.toLowerCase
          val table = tables.get(tableName)
          if (table == null) return SQLResult(true, s"Error: Table '$tableName' not found.")
          
          val columnsObj = insert.getColumns
          val valuesObj = insert.getValues
          val scalaExprs = valuesObj.getExpressions.asScala
          
          if (columnsObj == null && scalaExprs.length != table.columnOrder.length) {
              return SQLResult(true, s"Error: Column mismatch. Expected ${table.columnOrder.length} columns, got ${scalaExprs.length}.")
          }
          
          val finalValues = new Array[Any](table.columnOrder.length)

          for (i <- table.columnOrder.indices) {
             val colName = table.columnOrder(i)
             finalValues(i) = if (table.columns(colName).isString) "" else 0
          }

          if (columnsObj != null) {
             val specifiedCols = columnsObj.asScala.map(c => cleanColName(c.getFullyQualifiedName))
             
             if (specifiedCols.length != scalaExprs.length) {
                 return SQLResult(true, s"Error: Column mismatch. Specified ${specifiedCols.length} columns, but provided ${scalaExprs.length} values.")
             }
             
             var i = 0
             while (i < specifiedCols.length) {
                 val colName = specifiedCols(i)
                 val colIdx = table.columnOrder.indexOf(colName)
                 if (colIdx != -1) {
                     finalValues(colIdx) = scalaExprs(i) match {
                         case l: LongValue => 
                             if (table.columns(colName).isString) return SQLResult(true, s"Error: Column '$colName' expects String, but got Int.")
                             l.getValue.toInt
                         case s: StringValue => 
                             if (table.columns(colName).isVector) {
                                 // [NEW] Parse the incoming string into a Float Array for the Native Engine
                                 s.getValue.stripPrefix("[").stripSuffix("]").split(",").map(_.trim.toFloat)
                             } else if (!table.columns(colName).isString) {
                                 return SQLResult(true, s"Error: Column '$colName' expects Int, but got String.")
                             } else {
                                 s.getValue
                             }
                         case _ => if (table.columns(colName).isString) "" else 0
                     }
                 }
                 i += 1
             }
          } else {
             var i = 0
             while (i < scalaExprs.length) {
                 if (i < finalValues.length) {
                     val colName = table.columnOrder(i)
                     finalValues(i) = scalaExprs(i) match {
                         case l: LongValue => 
                             if (table.columns(colName).isString) return SQLResult(true, s"Error: Column '$colName' expects String, but got Int.")
                             l.getValue.toInt
                         case s: StringValue => 
                             if (!table.columns(colName).isString) return SQLResult(true, s"Error: Column '$colName' expects Int, but got String.")
                             s.getValue
                         case _ => if (table.columns(colName).isString) "" else 0
                     }
                 }
                 i += 1
             }
          }
          
          table.insertRow(finalValues)
          
          if (hasReturning) {
             val sb = new StringBuilder()
             sb.append(s"\n   Found Rows:\n")
             sb.append("   " + finalValues.mkString(" | ") + "\n")
             SQLResult(false, sb.toString(), 1L)
          } else {
             SQLResult(false, "Inserted 1 row.", 1L)
          }

        case create: CreateTable =>
          val tableName = create.getTable.getName.toLowerCase

          if (tables.containsKey(tableName)) {
            return SQLResult(true, s"Error: Table '$tableName' already exists.")
          }

          val newTable = new AwanTable(tableName, 1_000_000, dataDir = s"data/$tableName")

          val columns = create.getColumnDefinitions.asScala
          for (col <- columns) {
            val colName = col.getColumnName.toLowerCase 
            val dataType = col.getColDataType.getDataType.toUpperCase

            val isString = dataType.contains("CHAR") || dataType.contains("TEXT") || dataType.contains("STRING")
            val isVector = dataType.contains("VECTOR") // [NEW] Detect VECTOR type
            
            // Pass the isVector flag to the table schema
            newTable.addColumn(colName, isString = isString, isVector = isVector) 
          }

          register(tableName, newTable)
          SQLResult(false, s"Table '$tableName' created successfully.", 0L)

        case drop: Drop =>
          if (drop.getType.equalsIgnoreCase("TABLE")) {
             val tableName = drop.getName.getName.toLowerCase
             val table = tables.get(tableName)
             
             if (table == null) return SQLResult(true, s"Error: Table '$tableName' not found.")
             
             val dir = new java.io.File(table.dataDir)
             
             table.close() 
             tables.remove(tableName)
             
             def deleteRecursively(f: java.io.File): Unit = {
               if (f.isDirectory) {
                 val children = f.listFiles()
                 if (children != null) children.foreach(deleteRecursively)
               }
               f.delete()
             }
             deleteRecursively(dir)
             
             SQLResult(false, s"Table '$tableName' dropped.", 0L)
          } else {
             SQLResult(true, s"Error: Unsupported DROP type (${drop.getType}).")
          }

        case alter: Alter =>
          val tableName = alter.getTable.getName.toLowerCase
          val table = tables.get(tableName)
          if (table == null) return SQLResult(true, s"Error: Table '$tableName' not found.")

          val alterExprs = alter.getAlterExpressions.asScala
          
          val unsupported = alterExprs.find(expr => !expr.getOperation.name().equalsIgnoreCase("ADD"))
          if (unsupported.isDefined) {
             return SQLResult(true, s"Error: Unsupported ALTER operation (${unsupported.get.getOperation.name()}).")
          }

          for (expr <- alterExprs) {
             val colDataType = expr.getColDataTypeList.get(0)
             val colName = colDataType.getColumnName.toLowerCase 
             val dataType = colDataType.getColDataType.getDataType.toUpperCase
             
             val isString = dataType.contains("CHAR") || dataType.contains("TEXT") || dataType.contains("STRING")
             
             table.addColumn(colName, isString)
          }
          
          SQLResult(false, s"Table '$tableName' altered successfully.", 0L)

        case delete: Delete =>
          val tableName = delete.getTable.getName.toLowerCase
          val table = tables.get(tableName)
          if (table == null) return SQLResult(true, s"Error: Table '$tableName' not found.")
          
          val where = delete.getWhere
          val idsToDelete = if (where == null) {
             table.scanFilteredIds(table.columnOrder.head, 2, 0)
          } else {
             evaluateWhere(where, table)
          }
          
          var affected = 0L
          for (id <- idsToDelete) {
             if (table.delete(id)) affected += 1
          }
          
          SQLResult(false, s"Deleted $affected rows.", affected)

        case update: Update =>
           val tableName = update.getTable.getName.toLowerCase
           val table = tables.get(tableName)
           if (table == null) return SQLResult(true, s"Error: Table '$tableName' not found.")
           
           val where = update.getWhere
           val idsToUpdate = if (where == null) {
              table.scanFilteredIds(table.columnOrder.head, 2, 0)
           } else {
              evaluateWhere(where, table)
           }

           val cols = update.getColumns.asScala
           val exprs = update.getExpressions.asScala
           
           var affected = 0L
           val sb = new StringBuilder()
           if (hasReturning) sb.append(s"\n   Found Rows:\n")
           
           for (id <- idsToUpdate) {
              val oldRowOpt = table.getRow(id)
              if (oldRowOpt.isDefined) {
                 val row = oldRowOpt.get
                 for (i <- cols.indices) {
                    val rawName = cols(i).getFullyQualifiedName
                    val colName = cleanColName(rawName)
                    val colIdx = table.columnOrder.indexOf(colName)
                    if (colIdx != -1) {
                        exprs(i) match {
                            case l: LongValue => row(colIdx) = l.getValue.toInt
                            case s: StringValue => row(colIdx) = s.getValue
                            case _ => // Ignore unsupported types
                        }
                    }
                 }
                 table.delete(id)
                 table.insertRow(row)
                 affected += 1
                 
                 if (hasReturning) sb.append("   " + row.mkString(" | ") + "\n")
              }
           }
           
           if (hasReturning) {
              SQLResult(false, sb.toString(), affected)
           } else {
              SQLResult(false, s"Updated $affected rows.", affected)
           }

        case _ => SQLResult(true, "Error: Unsupported SQL statement.")
      }
    } catch {
      case e: Exception => SQLResult(true, s"SQL Error: ${e.getMessage}")
    }
  }
}