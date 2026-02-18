package org.awandb.core.ingest

import org.awandb.core.engine.AwanTable
import com.fasterxml.jackson.databind.ObjectMapper
import java.util.{List => JList, Map => JMap}
import scala.jdk.CollectionConverters._

object JsonShredder {
  
  private val mapper = new ObjectMapper()

  def ingest(jsonString: String, table: AwanTable): Int = {
    try {
      val list = mapper.readValue(jsonString, classOf[JList[JMap[String, AnyRef]]])
      var insertedCount = 0
      
      for (obj <- list.asScala) {
        val row = new Array[Any](table.columnOrder.length)
        
        for (i <- table.columnOrder.indices) {
          val colName = table.columnOrder(i)
          val isStringCol = table.columns(colName).isString 
          
          // [FIX] Use the recursive path resolver to handle dot-notation
          val value = resolvePath(obj, colName.split("\\."))
          
          row(i) = if (value == null) {
            if (isStringCol) "" else 0 
          } else {
            value match {
              case num: java.lang.Number => num.intValue()
              case str: String => str
              case other => other
            }
          }
        }
        
        table.insertRow(row)
        insertedCount += 1
      }
      
      insertedCount
      
    } catch {
      case e: Exception => 
        println(s"[JsonShredder] Error parsing payload: ${e.getMessage}")
        e.printStackTrace()
        0 
    }
  }

  /**
   * Recursively digs into a nested JSON object based on a dot-notation path.
   * e.g., ["metadata", "battery_level"]
   */
  private def resolvePath(obj: Any, path: Array[String], depth: Int = 0): Any = {
    if (obj == null || depth >= path.length) return null
    
    obj match {
      case map: JMap[_, _] =>
        // Safely cast to handle Jackson's raw Map output
        val stringMap = map.asInstanceOf[JMap[String, AnyRef]]
        val nextObj = stringMap.get(path(depth))
        
        // If we are at the end of the path, return the value. Otherwise, recurse.
        if (depth == path.length - 1) nextObj 
        else resolvePath(nextObj, path, depth + 1)
        
      case _ => null // Expected an object to dig into, but hit a primitive early
    }
  }
}