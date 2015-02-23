package com.dchekh

import java.util.concurrent.CountDownLatch
import java.util.Properties
import kafka.message._
import kafka.serializer._
import kafka.utils._
import kafka.consumer.ConsumerConfig
import kafka.consumer.Whitelist
import scala.collection.JavaConversions._
import org.apache.avro.Schema
import scala.collection.mutable.HashMap
import kafka.consumer.Consumer


class MyConsumer[T](mes: String, sleeptime: Int, cdl: CountDownLatch, cg_config: Properties) extends Runnable {

  val config = new ConsumerConfig(cg_config)
  val connector =  Consumer.create(config)
  val filterSpec = new Whitelist(cg_config.getProperty("topic"))

  val stream = connector.createMessageStreamsByFilter(filterSpec, 1).get(0)
  var list = List[String]()

  def run() {

    var message: List[String] = List()
    while (true) {
      read(bytes => {
        message = bytes
        //      println("scala  > received " + message)
//        val p1 = new ProcessingMSSQL(1,2)
//        println(p1.count(2))
      })
    }
    println(mes)
    cdl.countDown()
  }

  def read(write: List[String] => Unit) = {
    //    info("reading on stream now")
    var numMessages: Int = 0
    var numMessagesTotal: Int = 0
    for (messageAndTopic <- stream) //    while (!stream.isEmpty)
    {
      try {
        var m = messageAndTopic.message
        var part = messageAndTopic.partition 
        list = list ++ List(new String(m))
        numMessages += 1
        numMessagesTotal += 1
        AvroWrapper.decode(m) 
        
//        println(messageAndTopic.offset.toString + " : decode1 : " + AvroWrapper.decode(m,schema).toString() + "; partition - " + part + "; thread - " + mes)
//        println(messageAndTopic.offset.toString + AvroWrapper.decode(m) + "; partition - " + part + "; thread - " + mes)

//        val p1 = new ProcessingMSSQL(1,2)
//        println(p1.count(2))

        if (numMessages == 1000) {
          println("thread - " + mes + "; received messages: " + numMessagesTotal)

          numMessages = 0
          write(list)
          list = List()
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
