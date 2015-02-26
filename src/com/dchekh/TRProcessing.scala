package com.dchekh

import scala.collection.mutable.ArrayBuffer
import org.apache.avro.generic.{ GenericRecord }
import scala.collection.mutable.HashMap

trait TRProcessing {
  def run(x: ArrayBuffer[GenericRecord], topic_type: Int): Int
}

class Processing extends TRProcessing {
  var topic_type_ : Int = 0
  def run(messageArray: ArrayBuffer[GenericRecord], topic_type: Int): Int = {
    var result = 1
    topic_type_ = topic_type
    topic_type match {
      case 0 =>
        var pr = new ProcessingFirst().run(messageArray)
      case 1 =>
        var pr = new ProcessingSystem().run(messageArray)
      case _ => println(s"topic_type = not in (0,1)")
    }
    result
  }
}

class ProcessingSystem extends Processing {

  def run(messageArray: ArrayBuffer[GenericRecord]): Int = {
    var result = 1
    messageArray.foreach { x =>
      var schemaFields = x.getSchema.getFields
      var schemaDoc = x.getSchema.getDoc
      var a = 0
      var recordToMap = new HashMap[String, Any]
      for (a <- 0 until schemaFields.size()) {
        val field = schemaFields.get(a)
        val fieldName_ = field.name()
        val fieldValue = x.get(a).toString()
        fieldName_ match {
          case "action" => {
            fieldValue match {
              case "refresh" => {
                CFactory.schema_list = SchemaListObj.getSchemaList(CFactory.cfg_XML)
                println(s"topic_type = refresh")
              }
              case "add"     => println(s"topic_type = add")
              case "delete"  => println(s"topic_type = delete")
              case _         => println(s"topic_type is not in (refresh)")
            }
          }
          case _ => null//println(s" fieldName_  = $fieldValue ")
        }
      }
    }

    result
  }

}
