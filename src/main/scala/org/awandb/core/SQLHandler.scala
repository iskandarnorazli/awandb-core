/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.awandb.core.sql

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.update.Update
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.expression.operators.relational.{EqualsTo, ExpressionList}
import net.sf.jsqlparser.expression.{LongValue, StringValue}
import org.awandb.core.engine.AwanTable
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
          "SELECT not fully implemented in CLI. Use `query()` API for now."

        case insert: Insert =>
          val tableName = insert.getTable.getName.toLowerCase
          val table = tables.get(tableName)
          if (table == null) return s"Error: Table '$tableName' not found."

          val valuesObj = insert.getValues
          if (valuesObj == null) return "Error: INSERT statement missing VALUES clause."
          
          val expressionList = valuesObj.getExpressions
          if (expressionList == null) return "Error: Empty VALUES clause."

          val scalaExprs = expressionList.asScala

          // [FIX] Step 1: Validate first (Avoids non-local return in map)
          // We look for any expression that is NOT a Long or String
          val invalidExpr = scalaExprs.find {
            case _: LongValue => false
            case _: StringValue => false
            case _ => true
          }

          if (invalidExpr.isDefined) {
            return s"Error: Unsupported value type: ${invalidExpr.get}"
          }

          // [FIX] Step 2: Safe to Map now (No return needed here)
          val values = scalaExprs.map {
            case l: LongValue => l.getValue.toInt 
            case s: StringValue => s.getValue
            // No default case needed, we already filtered invalid ones
          }.toArray[Any]
          
          table.insertRow(values)
          "Inserted 1 row."

        case delete: Delete =>
          val tableName = delete.getTable.getName.toLowerCase
          val table = tables.get(tableName)
          if (table == null) return s"Error: Table '$tableName' not found."

          val where = delete.getWhere
          where match {
            case eq: EqualsTo =>
              val id = eq.getRightExpression.asInstanceOf[LongValue].getValue.toInt
              val success = table.delete(id)
              if (success) "Deleted 1 row." else "Row not found."
            case _ => "Error: DELETE only supports 'WHERE id = value'."
          }

        case update: Update =>
           val tableName = update.getTable.getName.toLowerCase
           val table = tables.get(tableName)
           if (table == null) return s"Error: Table '$tableName' not found."
           
           val where = update.getWhere
           where match {
             case eq: EqualsTo =>
               val id = eq.getRightExpression.asInstanceOf[LongValue].getValue.toInt
               val success = table.delete(id)
               if (success) "Updated (Deleted old row). Please INSERT new row." 
               else "Row not found."
               
             case _ => "Error: UPDATE only supports 'WHERE id = value'."
           }

        case _ => s"Error: Unsupported SQL statement."
      }
    } catch {
      case e: Exception => s"SQL Error: ${e.getMessage}"
    }
  }
}