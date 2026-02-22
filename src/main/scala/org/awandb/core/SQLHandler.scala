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

object SQLHandler {

  val tables = new java.util.concurrent.ConcurrentHashMap[String, AwanTable]()

  def register(name: String, table: AwanTable): Unit = {
    tables.put(name.toLowerCase, table)
  }

  def execute(sql: String): String = {
    try {
      val statement = CCJSqlParserUtil.parse(sql)

      statement match {
        case select: Select =>
          val plain = select.getSelectBody.asInstanceOf[PlainSelect]
          val leftTableName = plain.getFromItem.asInstanceOf[Table].getName.toLowerCase
          val leftTable = tables.get(leftTableName)
          if (leftTable == null) return s"Error: Table '$leftTableName' not found."

          // 1. CHECK FOR GROUP BY
          val groupBy = plain.getGroupBy
          if (groupBy != null) {
            val keyCol = groupBy.getGroupByExpressionList.getExpressions.get(0).asInstanceOf[Column].getColumnName
            var valCol = ""
            for (item <- plain.getSelectItems.asScala) {
               val expr = item.getExpression
               if (expr != null && expr.isInstanceOf[Function]) {
                  val func = expr.asInstanceOf[Function]
                  if (func.getName.equalsIgnoreCase("sum")) {
                     valCol = func.getParameters.getExpressions.get(0).asInstanceOf[Column].getColumnName
                  }
               }
            }
            if (valCol.isEmpty) return "Error: GROUP BY currently requires a SUM(column) aggregate."
            val resultMap = leftTable.executeGroupBy(keyCol, valCol)
            if (resultMap.isEmpty) return s"   GROUP BY Results: (Empty)"
            val sb = new StringBuilder()
            sb.append(s"\n   GROUP BY Results ($keyCol | SUM($valCol)):\n")
            resultMap.foreach { case (k, v) => sb.append(s"   $k | $v\n") }
            return sb.toString()
          }

          // 2. CHECK FOR JOIN
          val joins = plain.getJoins
          if (joins != null && !joins.isEmpty) {
            val join = joins.get(0)
            val rightTableName = join.getRightItem.asInstanceOf[Table].getName.toLowerCase
            val rightTable = tables.get(rightTableName)
            if (rightTable == null) return s"Error: Join Table '$rightTableName' not found."

            val onExpr = join.getOnExpressions.iterator().next().asInstanceOf[EqualsTo]
            val leftJoinCol = onExpr.getLeftExpression.asInstanceOf[Column].getColumnName
            val rightJoinCol = onExpr.getRightExpression.asInstanceOf[Column].getColumnName

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
          
          // [UPDATED] Evaluates to a primitive Array[Int] of Primary Keys
          def evaluateExpression(expr: Expression): Array[Int] = {
              expr match {
                 case p: Parenthesis => evaluateExpression(p.getExpression)
                 
                 case and: AndExpression =>
                    val leftIds = evaluateExpression(and.getLeftExpression)
                    // Fast Primitive Intersection
                    val rightIdsSet = evaluateExpression(and.getRightExpression).toSet
                    leftIds.filter(rightIdsSet.contains)
                    
                 case or: OrExpression =>
                    val leftIds = evaluateExpression(or.getLeftExpression)
                    val rightIds = evaluateExpression(or.getRightExpression)
                    // Fast Primitive Union
                    (leftIds.toSet ++ rightIds.toSet).toArray
                    
                 case eq: EqualsTo =>
                    val colName = eq.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase
                    val targetVal = eq.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                    leftTable.scanFilteredIds(colName, 0, targetVal)
                    
                 case gt: GreaterThan =>
                    val colName = gt.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase
                    val targetVal = gt.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                    leftTable.scanFilteredIds(colName, 1, targetVal)
                    
                 case gte: GreaterThanEquals =>
                     // ... [Keep existing opType 2 mapping] ...
                    val colName = gte.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase
                    val targetVal = gte.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                    leftTable.scanFilteredIds(colName, 2, targetVal)
                    
                 case lt: MinorThan =>
                    val colName = lt.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase
                    val targetVal = lt.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                    leftTable.scanFilteredIds(colName, 3, targetVal)
                    
                 case lte: MinorThanEquals =>
                    val colName = lte.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase
                    val targetVal = lte.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                    leftTable.scanFilteredIds(colName, 4, targetVal)
                    
                 case _ => throw new RuntimeException("Unsupported WHERE clause operator.")
              }
          }

          val where = plain.getWhere
          var finalRows = if (where == null) {
              // Bypass broken scanAll() by forcing an AVX scan
              leftTable.scanFiltered(leftTable.columnOrder.head, 2, 0).toSeq
          } else {
              try { 
                  // [LATE MATERIALIZATION]
                  // 1. Resolve tree into final surviving Row IDs
                  val matchedIds = evaluateExpression(where)
                  
                  // 2. Fetch payloads only for the survivors via O(1) PK Lookup
                  matchedIds.flatMap(id => leftTable.getRow(id)).toSeq
              } 
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
          table.insertRow(values)
          "Inserted 1 row."

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
          val success = table.delete(id)
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
           val oldRowOpt = table.getRow(id)
           if (oldRowOpt.isEmpty) return "Error: Row not found."
           
           val row = oldRowOpt.get
           val cols = update.getColumns.asScala
           val exprs = update.getExpressions.asScala
           
           for (i <- cols.indices) {
              val colName = cols(i).getColumnName
              val colIdx = table.columnOrder.indexOf(colName)
              if (colIdx != -1) {
                  // [FIX] Safely handle both Integers and Strings
                  exprs(i) match {
                      case l: LongValue => row(colIdx) = l.getValue.toInt
                      case s: StringValue => row(colIdx) = s.getValue
                      case _ => // Ignore unsupported types for now
                  }
              }
           }
           table.delete(id)
           table.insertRow(row)
           "Updated 1 row."

        case _ => s"Error: Unsupported SQL statement."
      }
    } catch {
      case e: Exception => s"SQL Error: ${e.getMessage}"
    }
  }
}