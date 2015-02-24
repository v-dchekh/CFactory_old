/**
 *
 */
package com.dchekh

import java.util.Properties
import kafka.utils.Logging
import java.util.concurrent.CountDownLatch
import org.apache.avro.Schema
import scala.collection.mutable.HashMap

class ConsumerGroup(threadNumber: Int = 5, a_zookeeper: String, a_groupId: String, topic: String, latch: CountDownLatch) extends Logging {

  //  info("setup:start topic=%s for zk=%s and groupId=%s".format(topic, a_zookeeper, a_groupId))

  def createConsumerConfig: Properties = {
    val props = new Properties()
    props.put("zookeeper.connect", a_zookeeper)
    props.put("group.id", a_groupId)
    props.put("zookeeper.session.timeout.ms", "4000")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    props.put("topic", topic)
    props
  }

  def launch {
    var cg_config = createConsumerConfig
    println(cg_config)

    for (i <- 1 to threadNumber by 1) {
      println("start Thread ****  groupId : " + a_groupId + ", thread : " + i)
      try {
        var consumer = new MyConsumer[String](":" + i,  latch, cg_config)
        new Thread(consumer).start()
      } catch {
        case e: Throwable =>
          if (true) error("Error creating new Consumer: ", e)
          else throw e
      }
    }
  }
}