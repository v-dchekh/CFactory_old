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


class ConsumerGroup(threadNumber: Int = 5, a_zookeeper: String, a_groupId: String, topic: String, latch : CountDownLatch) 
//extends Logging 
{

//  info("setup:start topic=%s for zk=%s and groupId=%s".format(topic, a_zookeeper, a_groupId))
  
  def createConsumerConfig(a_zookeeper: String, a_groupId: String): Properties = {
    val props = new Properties()
    props.put("zookeeper.connect", a_zookeeper)
    props.put("group.id", a_groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    props
  }

  def launch {
    var cg_config = createConsumerConfig(a_zookeeper, a_groupId)
    println(cg_config)

    for (i <-  1 to threadNumber by 1) {
      println("start Thread " +a_groupId +" : " + i)
      var consumer = new Consumer[String]("Thread.sleep " +a_groupId +" : " + i, i * 1000, latch)
      new Thread(consumer).start()

    }
  }
}