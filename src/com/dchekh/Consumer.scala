package com.dchekh

import java.util.concurrent.CountDownLatch
import java.util.Properties
import kafka.consumer.{ ConsumerConfig, Whitelist, Consumer }
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import org.apache.avro.Schema
import scala.collection.mutable.ArrayBuffer
import org.apache.avro.generic.{ GenericRecord }

class MyConsumer[T](mes: String, cdl: CountDownLatch, cg_config: Properties) extends Runnable {

  val config = new ConsumerConfig(cg_config)
  val connector = Consumer.create(config)
  val filterSpec = new Whitelist(cg_config.getProperty("topic"))

  val stream = connector.createMessageStreamsByFilter(filterSpec, 1).get(0)
  var list = List[String]()

  def run() {

    while (true) {
      read(bytes => {
        val p1 = new Processing().run(bytes)
      })
    }
    cdl.countDown()
  }

  def read(processing: ArrayBuffer[GenericRecord] => Unit) = {
    //-----------    info("reading on stream now")-------------//
    var numMessages: Int = 0
    var numMessagesTotal: Int = 0
    val messagArray = new ArrayBuffer[GenericRecord]()
    for (messageAndTopic <- stream) //    while (!stream.isEmpty)
    {
      try {
        var m = messageAndTopic.message
        var message = AvroWrapper.decode(m)
        var part = messageAndTopic.partition
        numMessages += 1
        numMessagesTotal += 1
        //println("topic : " + cg_config.getProperty("topic") + "--| " + messageAndTopic.offset.toString  + "; partition - " + part + "; thread - " + mes + s", total = $numMessagesTotal")
        messagArray += message

        if (numMessages == 3) {
        println("topic : " + cg_config.getProperty("topic") + "--| " + messageAndTopic.offset.toString  + s"; partition - $part , thread = $mes , total = $numMessagesTotal")
          processing(messagArray)
          numMessages = 0
          messagArray.clear()
        }
      } catch {
        case e: Throwable =>
          if (true)
            println("Error processing message, skipping this message: " + e)
          else throw e
      }
    }
  }

  def close() {
    connector.shutdown()
  }
}
