package com.dchekh

import scala.collection.mutable.ArrayBuffer
import org.apache.avro.generic.{ GenericRecord }

class ProcessingFirst extends Processing {
  override def run(messageArray: ArrayBuffer[GenericRecord]): Int = {
    var result = 1
    var count_ : Int = messageArray.size
    println(s"count_  : $count_" )
/*
    messageArray.foreach { x =>
      println("rec ----> " + x)
      var fl = x.getSchema.getFields
//      println("schema : " + x.getSchema)
//      println("fl.size() : " + fl.size())
      var a = 0
      for (a <- 0 until fl.size()) {
        println(s"a = $a , " + fl.get(a).name() + " = " + x.get(a))
      }

    }
    * 
    */
    result
  }

}