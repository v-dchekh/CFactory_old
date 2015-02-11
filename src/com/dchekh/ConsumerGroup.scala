/**
 *
 */
package com.dchekh

import kafka.message._
import kafka.serializer._
import java.util.Properties
import scala.collection.JavaConversions._
import kafka.utils.Logging
import java.util.concurrent.CountDownLatch;

class ConsumerGroup(threadNumber: Int = 5, a_zookeeper: String, a_groupId: String, topic: String, latch: CountDownLatch)

  extends Logging {

  //  info("setup:start topic=%s for zk=%s and groupId=%s".format(topic, a_zookeeper, a_groupId))

  def createConsumerConfig(a_zookeeper: String, a_groupId: String, topic: String): Properties = {
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
    var cg_config = createConsumerConfig(a_zookeeper, a_groupId, topic)
    println(cg_config)

    for (i <- 1 to threadNumber by 1) {
      println("start Thread ****  groupId : " + a_groupId + ", thread : " + i)
//      Thread.sleep(1)
      try {
        var consumer = new Consumer[String]("Thread :" + i, i * 1000, latch, cg_config)
        new Thread(consumer).start()
      } catch {
        case e: Throwable =>
          if (true) { //this is objective even how to conditionalize on it
            error("Error creating new Consumer: ", e)
          } else {
            throw e
          }
      }

    }
  }
}