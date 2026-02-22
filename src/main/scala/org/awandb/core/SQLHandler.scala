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
  MinorThanEquals
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

      // [NEW] Lifted and shared WHERE evaluator for Unrestricted DML
      // [NEW] Lifted and shared WHERE evaluator for Unrestricted DML
      def evaluateWhere(expr: Expression, table: AwanTable): Array[Int] = {
          
          // Helper to safely extract (ColumnName, TargetValue) and prevent ClassCastExceptions
          def getColAndVal(op: net.sf.jsqlparser.expression.operators.relational.ComparisonOperator): (String, Int) = {
             if (!op.getLeftExpression.isInstanceOf[Column]) {
                throw new RuntimeException("Left side of condition must be a column name.")
             }
             if (!op.getRightExpression.isInstanceOf[LongValue]) {
                throw new RuntimeException("Right side of condition must be an integer.")
             }
             (op.getLeftExpression.asInstanceOf[Column].getColumnName.toLowerCase, 
              op.getRightExpression.asInstanceOf[LongValue].getValue.toInt)
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
                
             case _ => throw new RuntimeException(s"Unsupported WHERE clause operator: ${expr.getClass.getSimpleName}")
          }
      }

      statement match {
        case select: Select =>
          val plain = select.getSelectBody.asInstanceOf[PlainSelect]
          val leftTableName =
            plain.getFromItem.asInstanceOf[Table].getName.toLowerCase
          val leftTable = tables.get(leftTableName)
          if (leftTable == null)
            return SQLResult(true, s"Error: Table '$leftTableName' not found.")

          // 1. CHECK FOR GROUP BY
          val groupBy = plain.getGroupBy
          if (groupBy != null) {
            val keyCol = groupBy.getGroupByExpressionList.getExpressions
              .get(0)
              .asInstanceOf[Column]
              .getColumnName
            var valCol = ""
            for (item <- plain.getSelectItems.asScala) {
              val expr = item.getExpression
              if (expr != null && expr.isInstanceOf[Function]) {
                val func = expr.asInstanceOf[Function]
                if (func.getName.equalsIgnoreCase("sum")) {
                  valCol = func.getParameters.getExpressions
                    .get(0)
                    .asInstanceOf[Column]
                    .getColumnName
                }
              }
            }
            if (valCol.isEmpty)
              return SQLResult(
                true,
                "Error: GROUP BY currently requires a SUM(column) aggregate."
              )
            val resultMap = leftTable.executeGroupBy(keyCol, valCol)
            if (resultMap.isEmpty)
              return SQLResult(false, "   GROUP BY Results: (Empty)", 0L)

            val sb = new StringBuilder()
            sb.append(s"\n   GROUP BY Results ($keyCol | SUM($valCol)):\n")
            resultMap.foreach { case (k, v) => sb.append(s"   $k | $v\n") }
            return SQLResult(false, sb.toString(), resultMap.size.toLong)
          }

          // 2. CHECK FOR JOIN
          val joins = plain.getJoins
          if (joins != null && !joins.isEmpty) {
            val join = joins.get(0)
            val rightTableName =
              join.getRightItem.asInstanceOf[Table].getName.toLowerCase
            val rightTable = tables.get(rightTableName)
            if (rightTable == null)
              return SQLResult(
                true,
                s"Error: Join Table '$rightTableName' not found."
              )

            val onExpr =
              join.getOnExpressions.iterator().next().asInstanceOf[EqualsTo]
            val leftJoinCol =
              onExpr.getLeftExpression.asInstanceOf[Column].getColumnName
            val rightJoinCol =
              onExpr.getRightExpression.asInstanceOf[Column].getColumnName

            val leftKeyIdx = leftTable.columnOrder.indexOf(leftJoinCol)
            val leftPayloadIdx = if (leftTable.columnOrder.length > 2) 2 else 0
            val rightKeyIdx = rightTable.columnOrder.indexOf(rightJoinCol)
            val rightPayloadIdx =
              if (rightTable.columnOrder.length > 1) 1 else 0

            val rightScan = new TableScanOperator(
              rightTable.blockManager,
              rightTable.blockManager.getLoadedBlocks.toArray,
              rightKeyIdx,
              rightPayloadIdx
            )
            val buildOp = new HashJoinBuildOperator(rightScan)
            val leftScan = new TableScanOperator(
              leftTable.blockManager,
              leftTable.blockManager.getLoadedBlocks.toArray,
              leftKeyIdx,
              leftPayloadIdx
            )
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
              NativeBridge.copyToScalaLong(
                batch.valuesPtr,
                payloads,
                batch.count
              )
              for (i <- 0 until math.min(batch.count, 10)) {
                sb.append(
                  s"   Match -> LeftKey: ${keys(i)} | RightPayload: ${payloads(i)}\n"
                )
              }
              batch = probeOp.next()
            }
            probeOp.close()
            sb.append(s"   Matches Found: $totalMatches\n")
            return SQLResult(false, sb.toString(), totalMatches.toLong)
          }

          // -------------------------------------------------------
          // 3. PIPELINE: FILTER -> SORT -> LIMIT -> PROJECT
          // -------------------------------------------------------

          def evaluateExpression(expr: Expression): Array[Int] = {
            expr match {
              case p: Parenthesis     => evaluateExpression(p.getExpression)
              case and: AndExpression =>
                val leftIds = evaluateExpression(and.getLeftExpression)
                val rightIdsSet = evaluateExpression(
                  and.getRightExpression
                ).toSet
                leftIds.filter(rightIdsSet.contains)
              case or: OrExpression =>
                val leftIds = evaluateExpression(or.getLeftExpression)
                val rightIds = evaluateExpression(or.getRightExpression)
                (leftIds.toSet ++ rightIds.toSet).toArray
              case eq: EqualsTo =>
                val colName = eq.getLeftExpression
                  .asInstanceOf[Column]
                  .getColumnName
                  .toLowerCase
                val targetVal =
                  eq.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                leftTable.scanFilteredIds(colName, 0, targetVal)
              case gt: GreaterThan =>
                val colName = gt.getLeftExpression
                  .asInstanceOf[Column]
                  .getColumnName
                  .toLowerCase
                val targetVal =
                  gt.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                leftTable.scanFilteredIds(colName, 1, targetVal)
              case gte: GreaterThanEquals =>
                val colName = gte.getLeftExpression
                  .asInstanceOf[Column]
                  .getColumnName
                  .toLowerCase
                val targetVal =
                  gte.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                leftTable.scanFilteredIds(colName, 2, targetVal)
              case lt: MinorThan =>
                val colName = lt.getLeftExpression
                  .asInstanceOf[Column]
                  .getColumnName
                  .toLowerCase
                val targetVal =
                  lt.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                leftTable.scanFilteredIds(colName, 3, targetVal)
              case lte: MinorThanEquals =>
                val colName = lte.getLeftExpression
                  .asInstanceOf[Column]
                  .getColumnName
                  .toLowerCase
                val targetVal =
                  lte.getRightExpression.asInstanceOf[LongValue].getValue.toInt
                leftTable.scanFilteredIds(colName, 4, targetVal)
              case _ =>
                throw new RuntimeException("Unsupported WHERE clause operator.")
            }
          }

          val where = plain.getWhere
          var finalRows = if (where == null) {
              leftTable.scanFiltered(leftTable.columnOrder.head, 2, 0).toSeq
          } else {
              try { 
                  // [FIX] Pass table as argument
                  val matchedIds = evaluateWhere(where, leftTable)
                  matchedIds.flatMap(id => leftTable.getRow(id)).toSeq
              } 
              catch { case e: RuntimeException => return SQLResult(true, "Error: " + e.getMessage) }
          }

          // --- SORT (ORDER BY) ---
          val orderByElements = plain.getOrderByElements
          if (orderByElements != null && !orderByElements.isEmpty) {
            val orderBy = orderByElements.get(0)
            val sortColName = orderBy.getExpression
              .asInstanceOf[Column]
              .getColumnName
              .toLowerCase
            val sortColIdx = leftTable.columnOrder.indexOf(sortColName)

            if (sortColIdx == -1)
              return SQLResult(
                true,
                s"Error: ORDER BY column '$sortColName' not found."
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
          val isAsterisk = plain.getSelectItems.get(0).toString == "*"

          if (isAsterisk) {
            requestedColumns ++= leftTable.columnOrder
          } else {
            for (item <- plain.getSelectItems.asScala) {
              val expr = item.getExpression
              if (expr != null && expr.isInstanceOf[Column]) {
                requestedColumns.append(
                  expr.asInstanceOf[Column].getColumnName.toLowerCase
                )
              }
            }
          }

          val projectionIndices = requestedColumns
            .map(col => leftTable.columnOrder.indexOf(col))
            .toArray
          if (projectionIndices.contains(-1))
            return SQLResult(
              true,
              s"Error: One or more projected columns do not exist."
            )

          val sb = new StringBuilder()
          sb.append(s"\n   Found Rows:\n")
          finalRows.foreach { row =>
            val projectedRow = projectionIndices.map(idx => row(idx))
            sb.append("   " + projectedRow.mkString(" | ") + "\n")
          }
          return SQLResult(false, sb.toString(), finalRows.length.toLong)

        case insert: Insert =>
          val tableName = insert.getTable.getName.toLowerCase
          val table = tables.get(tableName)
          if (table == null)
            return SQLResult(true, s"Error: Table '$tableName' not found.")

          val valuesObj = insert.getValues
          val scalaExprs = valuesObj.getExpressions.asScala
          val values = scalaExprs
            .map {
              case l: LongValue   => l.getValue.toInt
              case s: StringValue => s.getValue
              case _              => 0
            }
            .toArray[Any]

          table.insertRow(values)

          // [NEW] Check for RETURNING clause
          if (hasReturning) {
            val sb = new StringBuilder()
            sb.append(s"\n   Found Rows:\n")
            sb.append("   " + values.mkString(" | ") + "\n")
            SQLResult(false, sb.toString(), 1L)
          } else {
            SQLResult(false, "Inserted 1 row.", 1L)
          }

        case create: CreateTable =>
          val tableName = create.getTable.getName.toLowerCase

          if (tables.containsKey(tableName)) {
            return SQLResult(true, s"Error: Table '$tableName' already exists.")
          }

          // Instantiate a new AwanTable with a default capacity (e.g., 1 Million rows)
          // [FIX] Assign a unique physical storage directory per table!
          val newTable = new AwanTable(tableName, 1_000_000, dataDir = s"data/$tableName")

          val columns = create.getColumnDefinitions.asScala
          for (col <- columns) {
            val colName = col.getColumnName.toLowerCase
            val dataType = col.getColDataType.getDataType.toUpperCase

            // Map SQL types to AwanDB's internal binary types
            val isString = dataType.contains("CHAR") || dataType.contains(
              "TEXT"
            ) || dataType.contains("STRING")

            newTable.addColumn(colName, isString)
          }

          // Register the dynamically created table with the SQL Engine
          register(tableName, newTable)
          SQLResult(false, s"Table '$tableName' created successfully.", 0L)

        case drop: Drop =>
          if (drop.getType.equalsIgnoreCase("TABLE")) {
             val tableName = drop.getName.getName.toLowerCase
             val table = tables.get(tableName)
             
             if (table == null) return SQLResult(true, s"Error: Table '$tableName' not found.")
             
             // Capture the directory path before closing
             val dir = new java.io.File(table.dataDir)
             
             // Safely close the native C++ memory bounds
             table.close() 
             tables.remove(tableName)
             
             // [NEW] Physically wipe the data files from the SSD
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
          
          // [FIX] Pre-check for unsupported operations to avoid non-local returns
          val unsupported = alterExprs.find(expr => !expr.getOperation.name().equalsIgnoreCase("ADD"))
          if (unsupported.isDefined) {
             return SQLResult(true, s"Error: Unsupported ALTER operation (${unsupported.get.getOperation.name()}).")
          }

          // If we reach here, all operations are valid ADDs. Apply them safely.
          for (expr <- alterExprs) {
             val colDataType = expr.getColDataTypeList.get(0)
             val colName = colDataType.getColumnName.toLowerCase
             val dataType = colDataType.getColDataType.getDataType.toUpperCase
             
             // Map SQL types to AwanDB's binary engine
             val isString = dataType.contains("CHAR") || dataType.contains("TEXT") || dataType.contains("STRING")
             
             // Dynamically mutate the live AwanTable instance
             table.addColumn(colName, isString)
          }
          
          SQLResult(false, s"Table '$tableName' altered successfully.", 0L)

        case delete: Delete =>
          val tableName = delete.getTable.getName.toLowerCase
          val table = tables.get(tableName)
          if (table == null) return SQLResult(true, s"Error: Table '$tableName' not found.")
          
          val where = delete.getWhere
          val idsToDelete = if (where == null) {
             // If no WHERE clause, resolve all IDs using >= 0 on the Primary Key
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
                    val colName = cols(i).getColumnName.toLowerCase
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
