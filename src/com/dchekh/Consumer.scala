package com.dchekh

import java.util.concurrent.CountDownLatch;
import java.util.Properties
import kafka.message._
import kafka.serializer._
import kafka.utils._
import scala.collection.JavaConversions._
import kafka.consumer.ConsumerConfig
import kafka.consumer.Whitelist
import kafka.consumer.Consumer

class Consumer[T](mes: String, sleeptime: Int, cdl: CountDownLatch, cg_config: Properties) extends Runnable {

  val config = new ConsumerConfig(cg_config)
  val connector = Consumer.create(config)
  val filterSpec = new Whitelist(cg_config.getProperty("topic"))

  val stream = connector.createMessageStreamsByFilter(filterSpec, 1).get(0)
  var list = List[String]()

  def run() {

    //    println("topic" + cg_config.getProperty("topic"))

    var message: List[String] = List()
    while (true) {
      read(bytes => {
        message = bytes
//        println("scala  > received " + message)

      })
    }
    //    Thread.sleep(sleeptime)
    println(mes)
    cdl.countDown()

  }
  def read(write: List[String] => Unit) = {
    // def read  : List[String] = {
    //    info("reading on stream now")

    var numMessages: Int = 0
    for (messageAndTopic <- stream) //    while (!stream.isEmpty)
    {
      try {
        var m = messageAndTopic.message()
        var part = messageAndTopic.partition
        list = list ++ List(new String(m))
        numMessages += 1
        println(messageAndTopic.offset.toString + " : " + new String(m) + "; Partition - " + part+ "; thread - " +mes)

        if (numMessages == 3) {
          numMessages = 0
          write(list)
          list = List()
        }
      } catch {
        case e: Throwable =>
          if (true) { //this is objective even how to conditionalize on it
            error("Error processing message, skipping this message: " + e)
          } else {
            throw e
          }
      }
    }
  }

  def close() {
    connector.shutdown()
  }

}
