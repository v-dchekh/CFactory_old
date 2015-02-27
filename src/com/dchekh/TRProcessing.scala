package com.dchekh

import scala.collection.mutable.ArrayBuffer
import org.apache.avro.generic.{ GenericRecord }
import scala.collection.mutable.HashMap
import scala.xml.XML
import java.nio.file.{ Paths, Files }
import java.nio.charset.StandardCharsets

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
        val pr = new ProcessingSQLInsert().run(messageArray)
      case 1 =>
        val pr = new ProcessingSystem().run(messageArray)
      case _ => println(s"topic_type = not in (0,1)")
    }
    result
  }
}

class ProcessingSystem extends Processing {

  def createElement(attribute: Option[String]) = <schema file = { attribute.getOrElse(null) }/>
//<schema id = "5" file = "d:/Users/Dzmitry_Chekh/Scala_workspace/kafka_prod_cons/avro/user5.avsc" />

  private def schemasListRefresh {
    println(s"topic_type = refresh")
    CFactory.schema_list = SchemaListObj.getSchemaList(XML.loadFile(CFactory.filename))
  }
  private def schemasListAdd(fieldSchemaValue: String) {
    println(s"topic_type = add")
    val path = "d:/Users/Dzmitry_Chekh/Scala_workspace/kafka_prod_cons/avro/user5.avsc"
    Files.write(Paths.get(path), fieldSchemaValue.getBytes(StandardCharsets.UTF_8))
    println(createElement(Some(path)))
    schemasListRefresh
  }
  private def schemasListDelete {
    println(s"topic_type = delete")
    schemasListRefresh
  }

  def run(messageArray: ArrayBuffer[GenericRecord]): Int = {
    var result = 1
    messageArray.foreach { x =>
      val schemaFields = x.getSchema.getFields
      val schemaDoc = x.getSchema.getDoc
      val recordToMap = HashMap[String, Any]()
      val fieldAction = schemaFields.get(0)
      val fieldActionName = fieldAction.name()
      val fieldActionValue = x.get(0).toString()

      val fieldAvroSchema = schemaFields.get(1)
      val fieldAvroSchemaName = fieldAction.name()
      val fieldAvroSchemaValue = x.get(1).toString()

      fieldActionName match {
        case "action" => {
          fieldActionValue match {
            case "add"     => schemasListAdd(fieldAvroSchemaValue)
            case "delete"  => schemasListDelete
            case "refresh" => schemasListRefresh
            case _         => println(s"topic_type is not in (refresh)")
          }
          //println(CFactory.schema_list.mkString("\n"))
        }
      }
    }
    result
  }

}
