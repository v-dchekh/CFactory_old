package com.dchekh

import scala.collection.mutable.ArrayBuffer
import org.apache.avro.Schema
import java.io.File
import scala.collection.mutable.HashMap
import scala.xml.Elem

object SchemaListObj {

  def getSchemaList(cfg_XML: Elem) = {
    var schemas_list = (cfg_XML \\ "schemas" \\ "schema")
    var schena_list = new HashMap[String, Schema]
    var schema_id: String = null
    schemas_list.foreach { n =>
      var schema = Schema.parse(new File((n \ "@file").text))
      val m = Map("id" -> (n \ "@id").text, "schema" -> schema)
      schema_id = (n \ "@id").text
      schena_list += (schema_id -> schema)
    }
    schena_list
  }

  def getcons_groupList(cfg_XML: Elem) = {
    var cons_groupList = (cfg_XML \\ "consumer_groups" \\ "consumer_group")
    val groupList = new ArrayBuffer[Any]()

    var schema_id: String = null
    cons_groupList.foreach { n =>
      val groupId = (n \ "@groupId").text
      val zkconnect = (n \ "@zkconnect").text
      val topic = (n \ "@topic").text
      val thread_number = ((n \ "@thread_number").text).toInt

      val m = Map("groupId" -> groupId, "zkconnect" -> zkconnect, "topic" -> topic, "thread_number" -> thread_number)
      groupList += m

    }
    groupList
  }

  def getThread_number(cfg_XML: Elem) = {
    var cons_groupList = (cfg_XML \\ "consumer_groups" \\ "consumer_group")
    var thread_number: Int = 0
    cons_groupList.foreach { n =>
      thread_number = thread_number + ((n \ "@thread_number").text).toInt
    }
    thread_number
  }
}