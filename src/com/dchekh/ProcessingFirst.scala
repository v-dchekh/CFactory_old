package com.dchekh

import scala.collection.mutable.ArrayBuffer
import org.apache.avro.generic.{ GenericRecord }
import scala.collection.mutable.ListMap
import scala.collection.mutable.HashMap

class ProcessingFirst extends Processing {
  override def run(messageArray: ArrayBuffer[GenericRecord]): Int = {
    var result = 1
    var count_ : Int = messageArray.size

    messageArray.foreach { x =>
      println("rec ----> " + x)
      var fl = x.getSchema.getFields
      var a = 0
      var recordToMap = new HashMap[String, Any]
      for (a <- 0 until fl.size()) {
        val fieldName  = fl.get(a).name()
        val fieldOrder = fl.get(a).order()
        val fieldValue = x.get(a)
        val field      = fl.get(a)
        val fieldType  = fl.get(a).schema().getType
        println(s"a = $a , " + fieldName + " = " + fieldValue + s", fieldOrder = $fieldOrder, field -> $field, fieldType -> $fieldType ")
        recordToMap += (fieldName -> fieldValue)

      }
      val recordToMapSorted = recordToMap.toSeq.sortWith(_._1 < _._1)
      val recordToMapSorted2 = recordToMap.toSeq.sortWith(_._1 > _._1)
      println("MAP    ------: " + recordToMap)
      println("MAP S  ------: " + recordToMapSorted.toMap)
      println("MAP S2 ------: " + recordToMapSorted2.toMap)
      println("MAP ls ------: " + recordToMapSorted2.toMap.values.toList.mkString("('", "','", "')"))

    }
    result
  }

}