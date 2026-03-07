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
import net.sf.jsqlparser.expression.operators.arithmetic.{
  Addition,
  Subtraction,
  Multiplication,
  Division
}
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList
import net.sf.jsqlparser.statement.select.ParenthesedSelect
import org.awandb.core.jni.NativeBridge
import scala.jdk.CollectionConverters._

// Structured Result Class
case class SQLResult(
    isError: Boolean, 
    message: String, 
    affectedRows: Long = 0L,
    schema: Array[(String, String)] = Array.empty,
    columnarData: Array[Any] = Array.empty
)

sealed trait FilterAST {
  // Returns a flattened RPN instruction array
  def toRPN(table: AwanTable): Array[Int]
}

case class AndAST(left: FilterAST, right: FilterAST) extends FilterAST {
  // OP_AND = 2
  def toRPN(table: AwanTable): Array[Int] = left.toRPN(table) ++ right.toRPN(table) ++ Array(2)
}

case class OrAST(left: FilterAST, right: FilterAST) extends FilterAST {
  // OP_OR = 3
  def toRPN(table: AwanTable): Array[Int] = left.toRPN(table) ++ right.toRPN(table) ++ Array(3)
}

case class PredicateAST(colName: String, opType: Int, targetVal: Int) extends FilterAST {
  // OP_PRED = 1
  // Layout: [OP_CODE, COL_IDX, OP_TYPE, TARGET_VAL]
  def toRPN(table: AwanTable): Array[Int] = {
    val colIdx = table.columnOrder.indexOf(colName)
    if (colIdx == -1) throw new RuntimeException(s"Column '$colName' not found.")
    Array(1, colIdx, opType, targetVal)
  }
}

// Fallback for operations we haven't ported to AVX yet (like VECTOR_SEARCH or LIKE)
case class MaterializedAST(matchedIds: Array[Int]) extends FilterAST {
  // OP_MAT = 4
  // Layout: [OP_CODE, LENGTH, ID_1, ID_2, ... ID_N]
  def toRPN(table: AwanTable): Array[Int] = {
    Array(4, matchedIds.length) ++ matchedIds
  }
}

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
      def parseWhere(expr: Expression, table: AwanTable): FilterAST = {
         
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
            case p: Parenthesis => parseWhere(p.getExpression, table)
            
            // [NEW] Build AST instead of executing Scala set intersections
            case and: AndExpression =>
               AndAST(parseWhere(and.getLeftExpression, table), parseWhere(and.getRightExpression, table))
            case or: OrExpression =>
               OrAST(parseWhere(or.getLeftExpression, table), parseWhere(or.getRightExpression, table))
               
            case eq: EqualsTo =>
               val left = eq.getLeftExpression
               // VECTOR SEARCH INTERCEPTOR (Wrap in MaterializedAST)
               if (left.isInstanceOf[Function] && left.asInstanceOf[Function].getName.equalsIgnoreCase("VECTOR_SEARCH")) {
                  val func = left.asInstanceOf[Function]
                  val params = func.getParameters.getExpressions
                  
                  val rawCol = params.get(0) match {
                     case c: Column => c.getFullyQualifiedName
                     case _ => params.get(0).toString
                  }
                  val colName = cleanColName(rawCol)
                  
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
                  return MaterializedAST(table.queryVector(colName, vecArray, threshold))
               }

               val (colName, targetVal) = getColAndVal(eq)
               PredicateAST(colName, 0, targetVal)

            case gt: GreaterThan =>
               val (colName, targetVal) = getColAndVal(gt)
               PredicateAST(colName, 1, targetVal)
            case gte: GreaterThanEquals =>
               val (colName, targetVal) = getColAndVal(gte)
               PredicateAST(colName, 2, targetVal)
            case lt: MinorThan =>
               val (colName, targetVal) = getColAndVal(lt)
               PredicateAST(colName, 3, targetVal)
            case lte: MinorThanEquals =>
               val (colName, targetVal) = getColAndVal(lte)
               PredicateAST(colName, 4, targetVal)

            // Complex string operations fallback to eager materialization
            case in: InExpression =>
               val rawName = in.getLeftExpression.asInstanceOf[Column].getFullyQualifiedName
               val colName = cleanColName(rawName)
               if (!table.columnOrder.contains(colName)) throw new RuntimeException(s"Column '$colName' not found.")
               
               // [FIX] In JSqlParser 4.7+, we use getRightExpression() instead of getRightItemsList()
               val rightExpr = in.getRightExpression

               // Determine the target values by evaluating the right side of the IN clause
               val targetVals: Set[String] = rightExpr match {
                   
                   // SCENARIO 1: Standard List -> IN ('A', 'B', 'C')
                   case exprList: ParenthesedExpressionList[_] =>
                       // [FIX] ExpressionList now directly extends java.util.List
                       exprList.asScala.map {
                           case s: StringValue => s.getValue
                           case l: LongValue => l.getValue.toString
                           case e => e.toString
                       }.toSet

                   // SCENARIO 2: Recursive Subquery -> IN (SELECT id FROM other_table WHERE ...)
                   case subSelect: ParenthesedSelect =>
                       // [FIX] Extract the PlainSelect from the new Parenthesed wrapper
                       val plain = subSelect.getSelect.asInstanceOf[PlainSelect]
                       val subTableName = plain.getFromItem.asInstanceOf[Table].getName.toLowerCase
                       val subTable = tables.get(subTableName)
                       if (subTable == null) throw new RuntimeException(s"Subquery Error: Table '$subTableName' not found.")

                       // Identify which column the subquery is projecting
                       val selectItem = plain.getSelectItems.get(0)
                       val subColRaw = selectItem.getExpression.asInstanceOf[Column].getFullyQualifiedName
                       val subColName = cleanColName(subColRaw)
                       val subColIdx = subTable.columnOrder.indexOf(subColName)

                       if (subColIdx == -1) throw new RuntimeException(s"Subquery Error: Column '$subColName' not found.")

                       // RECURSION: Evaluate the subquery's WHERE clause to get matching IDs natively!
                       val subWhere = plain.getWhere
                       val subMatchedIds = if (subWhere == null) {
                           subTable.scanFilteredIds(subTable.columnOrder.head, 2, 0)
                       } else {
                           val subAst = parseWhere(subWhere, subTable) // Recursion happens here
                           subTable.executeCompositeFilter(subAst.toRPN(subTable))
                       }

                       // Extract the physical values from the subquery's matched rows
                       val vals = scala.collection.mutable.Set[String]()
                       for (id <- subMatchedIds) {
                           val rowOpt = subTable.getRow(id)
                           if (rowOpt.isDefined) {
                               vals.add(rowOpt.get(subColIdx).toString)
                           }
                       }
                       vals.toSet

                   case _ => throw new RuntimeException(s"Unsupported IN clause format: ${rightExpr.getClass.getSimpleName}")
               }

               // Finally, evaluate the Outer Query's IN filter using the resolved targetVals
               val allIds = table.scanFilteredIds(table.columnOrder.head, 2, 0)
               val colIdx = table.columnOrder.indexOf(colName)
               
               val matched = allIds.filter { id =>
                  val rowOpt = table.getRow(id)
                  rowOpt.isDefined && targetVals.contains(rowOpt.get(colIdx).toString)
               }
               
               MaterializedAST(matched)

            case like: LikeExpression =>
               val rawName = like.getLeftExpression.asInstanceOf[Column].getFullyQualifiedName
               val colName = cleanColName(rawName)
               if (!table.columnOrder.contains(colName)) throw new RuntimeException(s"Column '$colName' not found.")
               
               val targetVal = like.getRightExpression.asInstanceOf[StringValue].getValue
               val regexStr = "^" + targetVal.replace("%", ".*").replace("_", ".") + "$"
               val regex = regexStr.r
               
               val allIds = table.scanFilteredIds(table.columnOrder.head, 2, 0)
               val colIdx = table.columnOrder.indexOf(colName)
               
               val matched = allIds.filter { id =>
                  val rowOpt = table.getRow(id)
                  rowOpt.isDefined && regex.matches(rowOpt.get(colIdx).toString)
               }
               MaterializedAST(matched)

            case isNull: IsNullExpression =>
               val rawName = isNull.getLeftExpression.asInstanceOf[Column].getFullyQualifiedName
               val colName = cleanColName(rawName)
               if (!table.columnOrder.contains(colName)) throw new RuntimeException(s"Column '$colName' not found.")
               
               val isNot = isNull.isNot
               val allIds = table.scanFilteredIds(table.columnOrder.head, 2, 0)
               val colIdx = table.columnOrder.indexOf(colName)
               
               val matched = allIds.filter { id =>
                  val rowOpt = table.getRow(id)
                  if (rowOpt.isDefined) {
                        val v = rowOpt.get(colIdx)
                        val isNullEquivalent = v == null || v == "" || v == 0
                        if (isNot) !isNullEquivalent else isNullEquivalent
                  } else false
               }
               MaterializedAST(matched)
               
            case _ => throw new RuntimeException(s"Unsupported WHERE clause operator: ${expr.getClass.getSimpleName}")
         }
      }

      // ---------------------------------------------------------
      // QUERY ROUTER: O(1) PK vs O(N) AVX Pushdown
      // ---------------------------------------------------------
      def executeWhereFast(where: Expression, table: AwanTable): Array[Int] = {
        if (where == null) return table.scanFilteredIds(table.columnOrder.head, 2, 0)

        // O(1) PRIMARY KEY FAST-PATH
        where match {
          case eq: EqualsTo if eq.getLeftExpression.isInstanceOf[Column] && eq.getRightExpression.isInstanceOf[LongValue] =>
            val rawName = eq.getLeftExpression.asInstanceOf[Column].getFullyQualifiedName
            val colName = cleanColName(rawName)
            
            // If the query is exactly `WHERE id = X`, bypass AVX entirely and do a Hash Map lookup
            if (colName == table.columnOrder.head) {
              val targetVal = eq.getRightExpression.asInstanceOf[LongValue].getValue.toInt
              return if (table.getRow(targetVal).isDefined) Array(targetVal) else Array.empty[Int]
            }
          case _ => // Fall through to AST compiler
        }

        // Standard O(N) AVX Composite Filter Pushdown
        val ast = parseWhere(where, table)
        table.executeCompositeFilter(ast.toRPN(table))
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

            var finalResultSeq = resultMap.toSeq

            // ---------------------------------------------------------
            // [NEW] HAVING Clause Evaluator (Scala-side execution)
            // ---------------------------------------------------------
            val havingExpr = plain.getHaving
            if (havingExpr != null) {
                
                // Recursive evaluator for the HAVING AST
                def evaluateHaving(expr: Expression, k: Int, v: Long): Boolean = {
                    def extractValue(e: Expression): Long = e match {
                        case l: LongValue => l.getValue
                        case f: Function if f.getName.toUpperCase == "SUM" => v
                        case c: Column if cleanColName(c.getFullyQualifiedName) == keyCol => k.toLong
                        case p: Parenthesis => extractValue(p.getExpression)
                        case _ => throw new RuntimeException(s"Unsupported expression in HAVING: $e")
                    }

                    expr match {
                        case p: Parenthesis => evaluateHaving(p.getExpression, k, v)
                        case and: AndExpression => evaluateHaving(and.getLeftExpression, k, v) && evaluateHaving(and.getRightExpression, k, v)
                        case or: OrExpression => evaluateHaving(or.getLeftExpression, k, v) || evaluateHaving(or.getRightExpression, k, v)
                        
                        case gt: GreaterThan => extractValue(gt.getLeftExpression) > extractValue(gt.getRightExpression)
                        case gte: GreaterThanEquals => extractValue(gte.getLeftExpression) >= extractValue(gte.getRightExpression)
                        case lt: MinorThan => extractValue(lt.getLeftExpression) < extractValue(lt.getRightExpression)
                        case lte: MinorThanEquals => extractValue(lte.getLeftExpression) <= extractValue(lte.getRightExpression)
                        case eq: EqualsTo => extractValue(eq.getLeftExpression) == extractValue(eq.getRightExpression)
                        case neq: net.sf.jsqlparser.expression.operators.relational.NotEqualsTo => 
                             extractValue(neq.getLeftExpression) != extractValue(neq.getRightExpression)
                             
                        case _ => throw new RuntimeException(s"Unsupported operator in HAVING: ${expr.getClass.getSimpleName}")
                    }
                }

                // Filter the grouped results in memory
                finalResultSeq = finalResultSeq.filter { case (k, v) => evaluateHaving(havingExpr, k, v) }
            }

            // --- Apply ORDER BY and LIMIT to GROUP BY results ---
            // Handle ORDER BY
            val orderByElements = plain.getOrderByElements
            if (orderByElements != null && !orderByElements.isEmpty) {
              val orderBy = orderByElements.get(0)
              val isAsc = orderBy.isAsc
              val sortExprStr = orderBy.getExpression.toString.toLowerCase
              
              // Heuristic: If they sort by the Key column name, sort by Key. Otherwise, sort by the Value.
              if (sortExprStr.contains(keyCol.toLowerCase) || sortExprStr == "1") {
                 finalResultSeq = if (isAsc) finalResultSeq.sortBy(_._1) else finalResultSeq.sortBy(_._1).reverse
              } else {
                 finalResultSeq = if (isAsc) finalResultSeq.sortBy(_._2) else finalResultSeq.sortBy(_._2).reverse
              }
            }

            // Handle LIMIT
            val limit = plain.getLimit
            if (limit != null) {
              val limitExpr = limit.getRowCount
              if (limitExpr != null && limitExpr.isInstanceOf[LongValue]) {
                val limitVal = limitExpr.asInstanceOf[LongValue].getValue.toInt
                finalResultSeq = finalResultSeq.take(limitVal)
              }
            }
            
            val sb = new StringBuilder()
            sb.append(s"\n   GROUP BY Results ($rawKey | SUM($rawValCol)):\n")
            finalResultSeq.foreach { case (k, v) => sb.append(s"   $k | $v\n") }
            
            // 🚀 DYNAMIC COLUMNAR TRANSPOSITION FOR GROUP BY
            val outSchema = Array((keyCol, "INT"), (s"SUM($valCol)", "STRING"))
            val numRows = finalResultSeq.size
            val keyArr = new Array[Int](numRows)
            val valArr = new Array[String](numRows)
            
            var i = 0
            for ((k, v) <- finalResultSeq) {
                keyArr(i) = k
                valArr(i) = v.toString
                i += 1
            }
            val columnarData = Array[Any](keyArr, valArr)

            // Return the Native Columnar Arrays!
            return SQLResult(false, sb.toString(), numRows.toLong, outSchema, columnarData)
          }

          // 1.5. CHECK FOR GRAPH BFS
          var isGraphBfs = false
          var bfsStartNode = 0
          var srcCol = ""
          var dstCol = ""
          
          for (item <- plain.getSelectItems.asScala) {
            val expr = item.getExpression
            if (expr != null && expr.isInstanceOf[Function]) {
              val func = expr.asInstanceOf[Function]
              if (func.getName.equalsIgnoreCase("BFS_DISTANCE")) {
                isGraphBfs = true
                val params = func.getParameters.getExpressions
                
                // 1st Parameter: Start Node ID
                bfsStartNode = params.get(0).asInstanceOf[LongValue].getValue.toInt
                
                // 2nd and 3rd Parameters: Source and Destination Columns (Optional)
                if (params.size() >= 3) {
                  srcCol = cleanColName(params.get(1).asInstanceOf[Column].getFullyQualifiedName)
                  dstCol = cleanColName(params.get(2).asInstanceOf[Column].getFullyQualifiedName)
                } else {
                  // Fallback: assume the 2nd and 3rd columns in the schema are src and dst
                  if (leftTable.columnOrder.size >= 3) {
                     srcCol = leftTable.columnOrder(1)
                     dstCol = leftTable.columnOrder(2)
                  } else {
                     throw new RuntimeException("Table does not have enough columns for implicit BFS edges.")
                  }
                }
              }
            }
          }

          // GRAPH ENGINE INTERCEPTOR
          if (isGraphBfs) {
            try {
                // Dynamically wire the user's requested columns into the graph
                val graph = leftTable.projectToGraph(srcCol, dstCol)
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
            val leftPayloadIdx = leftTable.columnOrder.indices.find(i => 
                i != leftKeyIdx && !leftTable.columns(leftTable.columnOrder(i)).isString && !leftTable.columns(leftTable.columnOrder(i)).isVector
            ).getOrElse(leftKeyIdx)

            val rightKeyIdx = rightTable.columnOrder.indexOf(rightJoinCol)
            val rightPayloadIdx = rightTable.columnOrder.indices.find(i => 
                i != rightKeyIdx && !rightTable.columns(rightTable.columnOrder(i)).isString && !rightTable.columns(rightTable.columnOrder(i)).isVector
            ).getOrElse(rightKeyIdx)

            // [CRITICAL FIX] Wrap the entire JOIN execution to protect native blocks in both tables
            return leftTable.withEpoch {
              rightTable.withEpoch {
                var totalMatches = 0
                val sb = new StringBuilder()
                sb.append(s"\n   JOIN Results (Probe Output):\n")

                // --- 1. BUILD PHASE (Right Table - Disk Only) ---
                val rightScan = new TableScanOperator(rightTable.blockManager, rightTable.blockManager.getLoadedBlocks.toArray, rightKeyIdx, rightPayloadIdx)
                val buildOp = new HashJoinBuildOperator(rightScan)

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

                // --- 3. PROBE PHASE B (Left Table - RAM) ---
                val ramKeysBuffer = leftTable.columns(leftJoinCol).deltaIntBuffer
                val validRamKeys = new scala.collection.mutable.ArrayBuffer[Int]()
                
                for (i <- 0 until ramKeysBuffer.length) {
                    if (!leftTable.ramDeleted.get(i)) {
                        validRamKeys.append(ramKeysBuffer(i))
                    }
                }
                
                val mapPtr = buildOp.getMapPtr()
                if (mapPtr != 0L && validRamKeys.nonEmpty) {
                    val maxProbeSize = 4096
                    val maxFanOut = 100 // Handle up to 1:100 duplicate keys safely
                    val outCapacity = maxProbeSize * maxFanOut
                    
                    val probeKeysPtr = NativeBridge.allocMainStore(maxProbeSize)
                    val outPayloadsPtr = NativeBridge.allocMainStore(outCapacity * 2) 
                    val outIndicesPtr = NativeBridge.allocMainStore(outCapacity)
                    
                    var offset = 0
                    while (offset < validRamKeys.length) {
                        val chunkLen = math.min(maxProbeSize, validRamKeys.length - offset)
                        val chunk = validRamKeys.slice(offset, offset + chunkLen).toArray
                        
                        NativeBridge.loadData(probeKeysPtr, chunk)
                        
                        val ramMatches = NativeBridge.joinProbe(mapPtr, probeKeysPtr, chunkLen, outPayloadsPtr, outIndicesPtr)
                        totalMatches += ramMatches
                        
                        if (ramMatches > 0 && totalMatches <= 10) {
                            val toRead = math.min(ramMatches, 2)
                            val matchedPayloads = new Array[Long](toRead)
                            NativeBridge.copyToScalaLong(outPayloadsPtr, matchedPayloads, toRead)
                            for (i <- 0 until toRead) {
                               sb.append(s"   [RAM] Match -> RightPayload: ${matchedPayloads(i)}\n")
                            }
                        }
                        offset += chunkLen
                    }
                    
                    NativeBridge.freeMainStore(probeKeysPtr)
                    NativeBridge.freeMainStore(outPayloadsPtr)
                    NativeBridge.freeMainStore(outIndicesPtr)
                }
                
                // Clean up memory while still safely under epoch protection!
                probeOp.close()

                sb.append(s"   Total Matches Found: $totalMatches\n")
                SQLResult(false, sb.toString(), totalMatches.toLong)
              }
            }
          }

          // -------------------------------------------------------
          // 3. PIPELINE: ZERO-ALLOCATION AGGREGATION PUSHDOWN
          // -------------------------------------------------------
          val where = plain.getWhere
          
          val isAsterisk = plain.getSelectItems.get(0).toString == "*"
          
          // [FIX] Added a 4th String tuple element to hold the Alias
          val AGGREGATES = Set("SUM", "AVG", "MAX", "MIN", "COUNT")
          val pushdownAggregators = scala.collection.mutable.ArrayBuffer[(String, String, String, String)]() 
          
          // [NEW] Track scalar projections
          val scalarProjections = scala.collection.mutable.ArrayBuffer[(String, String, String)]() 
          var isPureAggregation = true 
          
          if (!isAsterisk) {
             for (item <- plain.getSelectItems.asScala) {
                 val expr = item.getExpression
                 val alias = if (item.getAlias != null) item.getAlias.getName else null

                 if (expr != null && expr.isInstanceOf[Function]) {
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

                     if (AGGREGATES.contains(funcName)) {
                         // It's a true aggregate (SUM, MAX, COUNT, etc.)
                         pushdownAggregators.append((funcName, cleanColName(rawParam), rawParam, alias))
                     } else {
                         // It's a row-level scalar (UPPER, ROUND, etc.)
                         isPureAggregation = false
                         scalarProjections.append((funcName, cleanColName(rawParam), alias))
                     }
                 } else {
                     isPureAggregation = false
                 }
             }
          } else {
             isPureAggregation = false
          }

          // FAST PATH: Pure Aggregation (No GROUP BY, No raw columns requested)
          if (isPureAggregation && groupBy == null && pushdownAggregators.nonEmpty) {
              // [FIX] Use the O(1) Router, but keep null check to preserve O(1) countAll()
              val matchedIds = if (where != null) executeWhereFast(where, leftTable) else null
              
              // O(1) Count resolution!
              val count = if (matchedIds != null) matchedIds.length.toLong else leftTable.countAll()
              
              // Memoization cache to prevent redundant loops for AVG and SUM mixes
              val computedSums = scala.collection.mutable.Map[String, Long]()
              def getSum(col: String): Long = {
                 computedSums.getOrElseUpdate(col, {
                    if (matchedIds != null) leftTable.sumFilteredIds(col, matchedIds)
                    else leftTable.sumColumn(col)
                 })
              }
              
              val finalAggRow = pushdownAggregators.map { case (funcName, cleanCol, rawCol, alias) =>
                   funcName match {
                       case "COUNT" => count.toString
                       case "SUM" => 
                          if (count == 0) "NULL" 
                          else getSum(cleanCol).toString
                       case "MAX" =>
                          if (count == 0) "NULL"
                          else {
                             if (matchedIds != null) leftTable.maxFilteredIds(cleanCol, matchedIds).toString
                             else leftTable.maxColumn(cleanCol).toString
                          }
                       case "MIN" =>
                          if (count == 0) "NULL"
                          else {
                             if (matchedIds != null) leftTable.minFilteredIds(cleanCol, matchedIds).toString
                             else leftTable.minColumn(cleanCol).toString
                          }
                       case "AVG" =>
                          if (count == 0) "NULL"
                          else (getSum(cleanCol).toDouble / count).toString
                       case _ => "NULL"
                   }
              }
              
              // [FIX] Use the alias if it exists, otherwise fallback to the function name
              val headers = pushdownAggregators.map { case (f, c, r, alias) => 
                 if (alias != null) alias 
                 else if (r == "*") s"$f(*)" 
                 else s"$f($r)" 
              }.mkString(" | ")
              
              val sb = new StringBuilder()
              sb.append(s"\n   Found Rows ($headers):\n")
              sb.append("   " + finalAggRow.mkString(" | ") + "\n")
              
              // 🚀 DYNAMIC COLUMNAR TRANSPOSITION FOR PURE AGGREGATIONS
              val outSchema = pushdownAggregators.map { case (f, c, r, alias) => 
                 val header = if (alias != null) alias else if (r == "*") s"$f(*)" else s"$f($r)"
                 (header, "STRING") 
              }.toArray
              
              val columnarData = finalAggRow.map(v => Array[String](v)).toArray[Any]

              // Return the Native Columnar Arrays!
              return SQLResult(false, sb.toString(), 1L, outSchema, columnarData)
          }

          // -------------------------------------------------------
          // 4. HEAVY PIPELINE: MATERIALIZE -> SORT -> LIMIT -> PROJECT
          // -------------------------------------------------------
          var finalRows = if (where == null) {
              leftTable.scanFiltered(leftTable.columnOrder.head, 2, 0).toSeq
          } else {
              try { 
                  // [FIX] Route to O(1) Interceptor
                  val matchedIds = executeWhereFast(where, leftTable)
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
              return SQLResult(true, s"Error: ORDER BY column '$rawSortName' not found.")

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
          
          // --- STANDARD PROJECTION (For non-aggregated queries) ---
          val requestedColumns = scala.collection.mutable.ArrayBuffer[String]()
          val requestedHeaders = scala.collection.mutable.ArrayBuffer[String]()
          
          // Note: We keep the old fallback aggregation code here just in case 
          // a query mixes scalar fields and aggregations improperly (e.g. SQLite style)
          val aggregators = scala.collection.mutable.ArrayBuffer[(String, String)]()
          var isAggregation = false
          
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
          
          // ---------------------------------------------------------
          // [NEW] DYNAMIC COLUMNAR TRANSPOSITION (For Arrow Zero-Copy)
          // ---------------------------------------------------------
          val outSchema = requestedColumns.zip(requestedHeaders).map { case (col, header) =>
            val isString = leftTable.columns(col).isString
            val isVector = leftTable.columns(col).isVector
            val typ = if (isVector) "VECTOR" else if (isString) "STRING" else "INT"
            (header, typ)
          }.toArray

          val numCols = projectionIndices.length
          val numRows = finalRows.length
          val columnarData = new Array[Any](numCols)
          
          for (c <- 0 until numCols) {
             val colName = requestedColumns(c)
             val isString = leftTable.columns(colName).isString
             val isVector = leftTable.columns(colName).isVector
             
             if (isVector) {
                val arr = new Array[Array[Float]](numRows)
                for (r <- 0 until numRows) arr(r) = finalRows(r)(projectionIndices(c)).asInstanceOf[Array[Float]]
                columnarData(c) = arr
             } else if (isString) {
                val arr = new Array[String](numRows)
                for (r <- 0 until numRows) arr(r) = finalRows(r)(projectionIndices(c)).toString
                columnarData(c) = arr
             } else {
                val arr = new Array[Int](numRows)
                for (r <- 0 until numRows) arr(r) = finalRows(r)(projectionIndices(c)) match {
                  case i: Int => i
                  case n: Number => n.intValue()
                  case _ => 0
                }
                columnarData(c) = arr
             }
          }

          // Legacy String Builder for standard backwards compatibility
          val sb = new StringBuilder()
          sb.append(s"\n   Found Rows (${requestedHeaders.mkString(" | ")}):\n")
          
          finalRows.foreach { row => 
             val projectedRow = projectionIndices.map(idx => row(idx))
             sb.append("   " + projectedRow.mkString(" | ") + "\n") 
          }
          
          // Return the new fully-loaded SQLResult!
          return SQLResult(false, sb.toString(), finalRows.length.toLong, outSchema, columnarData)

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
                             if (table.columns(colName).isString || table.columns(colName).isVector) return SQLResult(true, s"Error: Column '$colName' expects String/Vector, but got Int.")
                             l.getValue.toInt
                         case s: StringValue => 
                             if (table.columns(colName).isVector) {
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
                             if (table.columns(colName).isString || table.columns(colName).isVector) return SQLResult(true, s"Error: Column '$colName' expects String/Vector, but got Int.")
                             l.getValue.toInt
                         case s: StringValue => 
                             if (table.columns(colName).isVector) {
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
            val isVector = dataType.contains("VECTOR") // [CRITICAL FIX] Detect VECTOR type!
            
            newTable.addColumn(colName, isString = isString, isVector = isVector) 
          }

          register(tableName, newTable)
          SQLResult(false, s"Table '$tableName' created successfully.", 0L)

        case drop: Drop =>
          if (drop.getType.equalsIgnoreCase("TABLE")) {
             val tableName = drop.getName.getName.toLowerCase
             val table = tables.get(tableName)
             
             // 1. Clean up JVM memory if the table is currently loaded
             if (table != null) {
                 table.close() 
                 tables.remove(tableName)
             }
             
             // 2. ALWAYS wipe the directory from disk, even if the server just rebooted!
             // Fallback to the standard data directory pattern if table was null
             val dirPath = if (table != null) table.dataDir else s"data/$tableName"
             val dir = new java.io.File(dirPath)
             
             def deleteRecursively(f: java.io.File): Unit = {
               if (f.isDirectory) {
                 val children = f.listFiles()
                 if (children != null) children.foreach(deleteRecursively)
               }
               
               // Windows File-Lock Defeater
               if (f.exists() && !f.delete()) {
                   System.gc() // Force JVM to release any lingering streams
                   Thread.sleep(50) // Give the OS a millisecond to catch up
                   f.delete() // Try again!
               }
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
          
          // [FIX] Route to O(1) Interceptor for fast Primary Key deletion
          val idsToDelete = executeWhereFast(where, table)
          
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
           
           // [FIX] Route to O(1) Interceptor
           val idsToUpdate = executeWhereFast(where, table)

           val cols = update.getColumns.asScala
           val exprs = update.getExpressions.asScala
           
           var affected = 0L
           val sb = new StringBuilder()
           if (hasReturning) sb.append(s"\n   Found Rows:\n")

           // ---------------------------------------------------------
           // AVX MATH INTERCEPTOR (Fast Path)
           // ---------------------------------------------------------
           val standardCols = scala.collection.mutable.ArrayBuffer[Column]()
           val standardExprs = scala.collection.mutable.ArrayBuffer[Expression]()
           var mathPushedDown = false

           for (i <- cols.indices) {
              val rawName = cols(i).getFullyQualifiedName
              val colName = cleanColName(rawName)
              val expr = exprs(i)

              expr match {
                  case add: Addition if add.getRightExpression.isInstanceOf[LongValue] =>
                      table.executeMathUpdate(colName, '+', add.getRightExpression.asInstanceOf[LongValue].getValue.toInt, idsToUpdate)
                      mathPushedDown = true
                  case sub: Subtraction if sub.getRightExpression.isInstanceOf[LongValue] =>
                      table.executeMathUpdate(colName, '-', sub.getRightExpression.asInstanceOf[LongValue].getValue.toInt, idsToUpdate)
                      mathPushedDown = true
                  case mul: Multiplication if mul.getRightExpression.isInstanceOf[LongValue] =>
                      table.executeMathUpdate(colName, '*', mul.getRightExpression.asInstanceOf[LongValue].getValue.toInt, idsToUpdate)
                      mathPushedDown = true
                  case div: Division if div.getRightExpression.isInstanceOf[LongValue] =>
                      table.executeMathUpdate(colName, '/', div.getRightExpression.asInstanceOf[LongValue].getValue.toInt, idsToUpdate)
                      mathPushedDown = true
                  case _ =>
                      // Not a math operation, keep it for the standard row-by-row update loop
                      standardCols.append(cols(i))
                      standardExprs.append(expr)
              }
           }

           // If the entire UPDATE statement was just AVX math, skip the JVM loop entirely!
           if (standardCols.isEmpty && mathPushedDown) {
               affected = idsToUpdate.length
               if (hasReturning) {
                   for (id <- idsToUpdate) {
                       val rowOpt = table.getRow(id)
                       if (rowOpt.isDefined) sb.append("   " + rowOpt.get.mkString(" | ") + "\n")
                   }
                   return SQLResult(false, sb.toString(), affected)
               } else {
                   return SQLResult(false, s"Updated $affected rows (AVX Math Engine).", affected)
               }
           }

           // ---------------------------------------------------------
           // STANDARD TUPLE RECONSTRUCTION (Slow Path for Strings/Mixed)
           // ---------------------------------------------------------
           for (id <- idsToUpdate) {
              val oldRowOpt = table.getRow(id)
              if (oldRowOpt.isDefined) {
                 val row = oldRowOpt.get
                 for (i <- standardCols.indices) {
                    val rawName = standardCols(i).getFullyQualifiedName
                    val colName = cleanColName(rawName)
                    val colIdx = table.columnOrder.indexOf(colName)
                    if (colIdx != -1) {
                        standardExprs(i) match {
                            case l: LongValue => row(colIdx) = l.getValue.toInt
                            case s: StringValue => row(colIdx) = s.getValue
                            case _ => // Ignore unsupported types
                        }
                    }
                 }
                 // Physically delete and re-insert the modified row
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