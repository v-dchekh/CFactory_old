package com.dchekh

import scala.collection.mutable.ArrayBuffer
import org.apache.avro.generic.{ GenericRecord }

trait TRProcessing {
  def run(x: ArrayBuffer[GenericRecord]): Int
}

class Processing extends TRProcessing {
  def run (messageArray: ArrayBuffer[GenericRecord]): Int = {
    var result = 1
    var pr = new ProcessingFirst().run(messageArray)
    result
  }
}