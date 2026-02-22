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
import net.sf.jsqlparser.statement.select.{Select, PlainSelect, SelectItem, OrderByElement, Limit}
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.update.Update
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.expression.operators.relational.{EqualsTo, GreaterThan, GreaterThanEquals, MinorThan, MinorThanEquals}
import net.sf.jsqlparser.expression.operators.conditional.{AndExpression, OrExpression}
import net.sf.jsqlparser.expression.{LongValue, StringValue, Function, Expression, Parenthesis}
import net.sf.jsqlparser.schema.{Column, Table}
import org.awandb.core.engine.AwanTable
import org.awandb.core.query.{HashJoinBuildOperator, HashJoinProbeOperator, TableScanOperator}
import org.awandb.core.jni.NativeBridge
import scala.jdk.CollectionConverters._ 
import org.awandb.core.pipeline.PipeManager
import scala.util.matching.Regex

object SQLHandler {

  val tables = new java.util.concurrent.ConcurrentHashMap[String, AwanTable]()

  // Regex for continuous ETL piping
  val CreatePipePattern: Regex = """(?i)CREATE\s+PIPE\s+(\w+)\s+AS\s+SELECT\s+\*\s+FROM\s+(\w+)\s+INTO\s+(\w+)""".r

  def register(name: String, table: AwanTable): Unit = {
    tables.put(name.toLowerCase, table)
  }

  def execute(sql: String): String = {
    val trimmedSql = sql.trim
    
    // ====================================================================
    // 1. FAST-PATH DDL INTERCEPTORS (Bypassing JSqlParser)
    // ====================================================================
    trimmedSql match {
      case CreatePipePattern(pipeName, sourceName, destName) =>
        val sourceTable = tables.get(sourceName.toLowerCase)
        val destTable = tables.get(destName.toLowerCase)
        
        if (sourceTable == null) return s"Error: Source table '$sourceName' not found."
        if (destTable == null) return s"Error: Destination table '$destName' not found."
        
        try {
          PipeManager.startPipe(pipeName, sourceTable, destTable)
          return s"Status: Pipe '$pipeName' created and running in background."
        } catch {
          case e: Exception => return s"Error starting pipe: ${e.getMessage}"
        }
        
      case _ => // Fall through to standard JSqlParser execution
    }

    // ====================================================================
    // 2. STANDARD ANSI SQL EXECUTION
    // ====================================================================
    try {
      val statement = CCJSqlParserUtil.parse(trimmedSql)

      statement match {
        case select: Select =>
          val plain = select.getSelectBody.asInstanceOf[PlainSelect]
          val leftTableName = plain.getFromItem.asInstanceOf[Table].getName.toLowerCase
          val leftTable = tables.get(leftTableName)
          if (leftTable == null) return s"Error: Table '$leftTableName' not found."

          // ====================================================================
          // ROUTER: JOINS & AGGREGATIONS
          // ====================================================================
          val joins = plain.getJoins
          val groupBy = plain.getGroupBy

          // --------------------------------------------------------------------
          // PATH 1: STAR SCHEMA (JOIN + GROUP BY)
          // Uses the Fused Join-Aggregate C++ God Kernel
          // --------------------------------------------------------------------
          if (joins != null && !joins.isEmpty && groupBy != null) {
            val join = joins.get(0)
            val rightTableName = join.getRightItem.asInstanceOf[Table].getName.toLowerCase
            val rightTable = tables.get(rightTableName)
            if (rightTable == null) return s"Error: Dimension Table '$rightTableName' not found."

            // Parse ON Clause (e.g., ON fact.dim_id = dim.id)
            val onExpr = join.getOnExpressions.iterator().next().asInstanceOf[EqualsTo]
            val leftJoinCol = onExpr.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase
            val rightJoinCol = onExpr.getRightExpression.asInstanceOf[Column].getColumnName.toLowerCase

            // Parse GROUP BY Clause (The Dimension Attribute)
            val groupCol = groupBy.getGroupByExpressionList.getExpressions.get(0).asInstanceOf[Column].getColumnName.toLowerCase

            // Parse SELECT SUM() Clause (The Fact Measure)
            var sumCol = ""
            for (item <- plain.getSelectItems.asScala) {
               val expr = item.getExpression
               if (expr != null && expr.isInstanceOf[Function]) {
                  val func = expr.asInstanceOf[Function]
                  if (func.getName.equalsIgnoreCase("sum")) {
                     sumCol = func.getParameters.getExpressions.get(0).asInstanceOf[Column].getColumnName.toLowerCase
                  }
               }
            }
            if (sumCol.isEmpty) return "Error: Star queries require a SUM(column) aggregate."

            try {
              val resultMap = leftTable.executeStarQuery(rightTable, rightJoinCol, groupCol, leftJoinCol, sumCol)
              
              if (resultMap.isEmpty) return s"\n   STAR QUERY Results: (Empty)\n"
              val sb = new StringBuilder()
              sb.append(s"\n   STAR QUERY Results ($groupCol | SUM($sumCol)):\n")
              resultMap.foreach { case (k, v) => sb.append(s"   $k | $v\n") }
              return sb.toString()
            } catch {
              case e: Exception => return s"Error executing Star Query: ${e.getMessage}"
            }
          }
          
          // --------------------------------------------------------------------
          // PATH 2: PURE AGGREGATION (GROUP BY only)
          // Uses the Fused Filter-Aggregate C++ Kernel
          // --------------------------------------------------------------------
          else if (groupBy != null) {
            val keyCol = groupBy.getGroupByExpressionList.getExpressions.get(0).asInstanceOf[Column].getColumnName.toLowerCase
            var valCol = ""
            for (item <- plain.getSelectItems.asScala) {
               val expr = item.getExpression
               if (expr != null && expr.isInstanceOf[Function]) {
                  val func = expr.asInstanceOf[Function]
                  if (func.getName.equalsIgnoreCase("sum")) {
                     valCol = func.getParameters.getExpressions.get(0).asInstanceOf[Column].getColumnName.toLowerCase
                  }
               }
            }
            if (valCol.isEmpty) return "Error: GROUP BY currently requires a SUM(column) aggregate."

            val grpWhere = plain.getWhere 
            val resultMap = if (grpWhere == null) {
              leftTable.executeGroupBy(keyCol, valCol)
            } else {
              try {
                grpWhere match { 
                  case eq: EqualsTo => leftTable.executeFilteredGroupBy(keyCol, valCol, eq.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase, 0, eq.getRightExpression.asInstanceOf[LongValue].getValue.toInt)
                  case gt: GreaterThan => leftTable.executeFilteredGroupBy(keyCol, valCol, gt.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase, 1, gt.getRightExpression.asInstanceOf[LongValue].getValue.toInt)
                  case gte: GreaterThanEquals => leftTable.executeFilteredGroupBy(keyCol, valCol, gte.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase, 2, gte.getRightExpression.asInstanceOf[LongValue].getValue.toInt)
                  case lt: MinorThan => leftTable.executeFilteredGroupBy(keyCol, valCol, lt.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase, 3, lt.getRightExpression.asInstanceOf[LongValue].getValue.toInt)
                  case lte: MinorThanEquals => leftTable.executeFilteredGroupBy(keyCol, valCol, lte.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase, 4, lte.getRightExpression.asInstanceOf[LongValue].getValue.toInt)
                  case _ => throw new RuntimeException("Unsupported WHERE clause operator in GROUP BY.")
                }
              } catch {
                case e: RuntimeException => return s"Error: ${e.getMessage}"
              }
            }

            if (resultMap.isEmpty) return s"   GROUP BY Results: (Empty)"
            val sb = new StringBuilder()
            sb.append(s"\n   GROUP BY Results ($keyCol | SUM($valCol)):\n")
            resultMap.foreach { case (k, v) => sb.append(s"   $k | $v\n") }
            return sb.toString()
          }

          // --------------------------------------------------------------------
          // PATH 3: PURE JOIN (No Aggregation)
          // Uses Volcano Materialized Execution
          // --------------------------------------------------------------------
          else if (joins != null && !joins.isEmpty) {
            val join = joins.get(0)
            val rightTableName = join.getRightItem.asInstanceOf[Table].getName.toLowerCase
            val rightTable = tables.get(rightTableName)
            if (rightTable == null) return s"Error: Join Table '$rightTableName' not found."

            val onExpr = join.getOnExpressions.iterator().next().asInstanceOf[EqualsTo]
            val leftJoinCol = onExpr.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase
            val rightJoinCol = onExpr.getRightExpression.asInstanceOf[Column].getColumnName.toLowerCase

            val leftKeyIdx = leftTable.columnOrder.indexOf(leftJoinCol)
            val leftPayloadIdx = if (leftTable.columnOrder.length > 2) 2 else 0 
            val rightKeyIdx = rightTable.columnOrder.indexOf(rightJoinCol)
            val rightPayloadIdx = if (rightTable.columnOrder.length > 1) 1 else 0

            val rightScan = new TableScanOperator(rightTable.blockManager, rightTable.blockManager.getLoadedBlocks.toArray, rightKeyIdx, rightPayloadIdx)
            val buildOp = new HashJoinBuildOperator(rightScan)
            val leftScan = new TableScanOperator(leftTable.blockManager, leftTable.blockManager.getLoadedBlocks.toArray, leftKeyIdx, leftPayloadIdx)
            val probeOp = new HashJoinProbeOperator(leftScan, buildOp)

            probeOp.open()
            var totalMatches = 0
            var batch = probeOp.next()
            val sb = new StringBuilder()
            sb.append(s"\n   JOIN Results (Probe Output):\n")
            while (batch != null) {
               totalMatches += batch.count
               val keys = new Array[Int](batch.count)
               val payloads = new Array[Long](batch.count)
               NativeBridge.copyToScala(batch.keysPtr, keys, batch.count)
               NativeBridge.copyToScalaLong(batch.valuesPtr, payloads, batch.count)
               for (i <- 0 until math.min(batch.count, 10)) {
                  sb.append(s"   Match -> LeftKey: ${keys(i)} | RightPayload: ${payloads(i)}\n")
               }
               batch = probeOp.next()
            }
            probeOp.close()
            sb.append(s"   Matches Found: $totalMatches\n")
            return sb.toString()
          }

          // -------------------------------------------------------
          // 3. PIPELINE: FILTER -> SORT -> LIMIT -> PROJECT
          // -------------------------------------------------------
          
          def evaluateExpression(expr: Expression): Seq[Array[Any]] = {
              expr match {
                 case p: Parenthesis => evaluateExpression(p.getExpression)
                 case and: AndExpression =>
                    val leftRows = evaluateExpression(and.getLeftExpression)
                    val rightIds = evaluateExpression(and.getRightExpression).map(_(0)).toSet
                    leftRows.filter(r => rightIds.contains(r(0)))
                 case or: OrExpression =>
                    val leftRows = evaluateExpression(or.getLeftExpression)
                    val rightRows = evaluateExpression(or.getRightExpression)
                    val leftIds = leftRows.map(_(0)).toSet
                    leftRows ++ rightRows.filterNot(r => leftIds.contains(r(0)))
                 case eq: EqualsTo =>
                    val colName = eq.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase
                    val targetVal = eq.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                    leftTable.scanFiltered(colName, 0, targetVal).toSeq
                 case gt: GreaterThan =>
                    val colName = gt.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase
                    val targetVal = gt.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                    leftTable.scanFiltered(colName, 1, targetVal).toSeq
                 case gte: GreaterThanEquals =>
                    val colName = gte.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase
                    val targetVal = gte.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                    leftTable.scanFiltered(colName, 2, targetVal).toSeq
                 case lt: MinorThan =>
                    val colName = lt.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase
                    val targetVal = lt.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                    leftTable.scanFiltered(colName, 3, targetVal).toSeq
                 case lte: MinorThanEquals =>
                    val colName = lte.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase
                    val targetVal = lte.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                    leftTable.scanFiltered(colName, 4, targetVal).toSeq
                 case _ => throw new RuntimeException("Unsupported WHERE clause operator.")
              }
          }

          val where = plain.getWhere
          var finalRows = if (where == null) {
              // [FIX] Bypass broken scanAll() by forcing an AVX scan
              leftTable.scanFiltered(leftTable.columnOrder.head, 2, 0).toSeq
          } else {
              try { evaluateExpression(where) } 
              catch { case e: RuntimeException => return "Error: " + e.getMessage }
          }

          // --- SORT (ORDER BY) ---
          val orderByElements = plain.getOrderByElements
          if (orderByElements != null && !orderByElements.isEmpty) {
              val orderBy = orderByElements.get(0)
              val sortColName = orderBy.getExpression.asInstanceOf[Column].getColumnName.toLowerCase
              val sortColIdx = leftTable.columnOrder.indexOf(sortColName)
              
              if (sortColIdx == -1) return s"Error: ORDER BY column '$sortColName' not found."
              
              val isAsc = orderBy.isAsc
              finalRows = finalRows.sortWith { (r1, r2) =>
                  val cmp = (r1(sortColIdx), r2(sortColIdx)) match {
                      case (i1: Int, i2: Int) => i1.compareTo(i2)
                      case (l1: Long, l2: Long) => l1.compareTo(l2)
                      case (s1: String, s2: String) => s1.compareTo(s2)
                      case _ => 0
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
          val isAsterisk = plain.getSelectItems.get(0).toString == "*"
          
          if (isAsterisk) {
             requestedColumns ++= leftTable.columnOrder
          } else {
             for (item <- plain.getSelectItems.asScala) {
                 val expr = item.getExpression
                 if (expr != null && expr.isInstanceOf[Column]) {
                     requestedColumns.append(expr.asInstanceOf[Column].getColumnName.toLowerCase)
                 }
             }
          }
          
          val projectionIndices = requestedColumns.map(col => leftTable.columnOrder.indexOf(col)).toArray
          if (projectionIndices.contains(-1)) return s"Error: One or more projected columns do not exist."
          
          val sb = new StringBuilder()
          sb.append(s"\n   Found Rows:\n")
          finalRows.foreach { row => 
             val projectedRow = projectionIndices.map(idx => row(idx))
             sb.append("   " + projectedRow.mkString(" | ") + "\n") 
          }
          return sb.toString()

        case insert: Insert =>
          val tableName = insert.getTable.getName.toLowerCase
          val table = tables.get(tableName)
          if (table == null) return s"Error: Table '$tableName' not found."
          
          val valuesObj = insert.getValues
          val scalaExprs = valuesObj.getExpressions.asScala
          val values = scalaExprs.map {
            case l: LongValue => l.getValue.toInt 
            case s: StringValue => s.getValue
            case _ => 0
          }.toArray[Any]
          
          // [FIXED] Catch the strict schema validation errors from Native insertRow!
          try {
            table.insertRow(values)
            "Inserted 1 row."
          } catch {
            case e: IllegalArgumentException => s"Error: ${e.getMessage}"
            case e: Exception => s"Error: ${e.getMessage}"
          }

        case delete: Delete =>
          val tableName = delete.getTable.getName.toLowerCase
          val table = tables.get(tableName)
          if (table == null) return s"Error: Table '$tableName' not found."
          
          val where = delete.getWhere
          if (where == null || !where.isInstanceOf[EqualsTo]) return "Error: DELETE only supports 'WHERE id = value'."
          
          val eq = where.asInstanceOf[EqualsTo]
          val rightExpr = eq.getRightExpression
          if (!rightExpr.isInstanceOf[LongValue]) return "Error: DELETE only supports integer IDs."
          
          val id = rightExpr.asInstanceOf[LongValue].getValue.toInt
          val success = table.delete(id) // Now an O(1) lock-free operation!
          if (success) "Deleted 1 row." else "Row not found."

        case update: Update =>
           val tableName = update.getTable.getName.toLowerCase
           val table = tables.get(tableName)
           if (table == null) return s"Error: Table '$tableName' not found."
           
           val where = update.getWhere
           if (where == null || !where.isInstanceOf[EqualsTo]) return "Error: UPDATE only supports 'WHERE id = value'."
           
           val eq = where.asInstanceOf[EqualsTo]
           val rightExpr = eq.getRightExpression
           if (!rightExpr.isInstanceOf[LongValue]) return "Error: UPDATE only supports integer IDs."
           
           val id = rightExpr.asInstanceOf[LongValue].getValue.toInt
           
           val cols = update.getColumns.asScala
           val exprs = update.getExpressions.asScala
           
           // [FIXED] Build a Map of changes to pass to the Native C++ Mutator
           var changes = Map[String, Any]()
           for (i <- cols.indices) {
              val colName = cols(i).getColumnName
              exprs(i) match {
                  case l: LongValue => changes += (colName -> l.getValue.toInt)
                  case s: StringValue => changes += (colName -> s.getValue)
                  case _ => // Ignore unsupported types
              }
           }
           
           // Trigger the C++ God Kernel memory overwrite
           val success = table.update(id, changes)
           if (success) "Updated 1 row." else "Error: Row not found."

        case _ => s"Error: Unsupported SQL statement."
      }
    } catch {
      case e: Exception => s"SQL Error: ${e.getMessage}"
    }
  }
}