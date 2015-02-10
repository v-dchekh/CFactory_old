
package com.dchekh

import java.util.Properties
import scala.xml.XML
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

object CFactory {

  val usage = """
Usage: parser [-v] [-f file] [-s sopt] ...
Where: -v   Run verbosely
       -f F Set input file to F
       -s S Set Show option to S
"""

  var filename: String = ""
  var showme: String = ""
  var debug: Boolean = false
  val unknown = "(^-[^\\s])".r

  val pf: PartialFunction[List[String], List[String]] = {
    case "-v" :: tail =>
      debug = true; tail
    case "-f" :: (arg: String) :: tail =>
      filename = arg; tail
    case "-s" :: (arg: String) :: tail =>
      showme = arg; tail
    case unknown(bad) :: tail => endOfJob("unknown argument " + bad + "\n" + usage)
  }

  def parseArgs(args: List[String], pf: PartialFunction[List[String], List[String]]): List[String] = args match {
    case Nil => Nil
    case _   => if (pf isDefinedAt args) parseArgs(pf(args), pf) else args.head :: parseArgs(args.tail, pf)
  }

  def endOfJob(msg: String = usage) = {
    println(msg)
    sys.exit(1)
  }

  def main(args: Array[String]) {
    println("CFactory v0.1")
    // if there are required args:
    //    if (args.length == 0) die()
    val arglist = args.toList
    val remainingopts = parseArgs(arglist, pf)

    if (filename.length == 0) filename = "d:/Users/Dzmitry_Chekh/Scala_workspace/CFactory/bin/consumer_groups.xml"

    println("debug=" + debug)
    println("showme=" + showme)
    println("filename=" + filename)
    println("remainingopts=" + remainingopts)

    //"d:/Users/Dzmitry_Chekh/Scala_workspace/CFactory/bin/consumer_groups.xml"
    var cons_groups_config_XML = XML.loadFile(filename)
    var cons_groupList = (cons_groups_config_XML \\ "consumer_group")
    var groupId: String = null
    var zkconnect: String = null
    var topic: String = null
    var thread_number: Int = 0

    cons_groupList.foreach { n =>
      thread_number = thread_number + ((n \ "@thread_number").text).toInt
    }

    val  latch = new CountDownLatch(thread_number)

    val parlist = Map()
    val key : String = "" 
    val value : String = "" 
    
    
    
    cons_groupList.foreach { n =>
      groupId = (n \ "@groupId").text
      zkconnect = (n \ "@zkconnect").text
      topic = (n \ "@topic").text
      thread_number = ((n \ "@thread_number").text).toInt
      
      
      //println(s"$groupId, $zkconnect, $topic")
      val cg = new ConsumerGroup(thread_number, zkconnect, groupId, topic, latch).launch
    }

    latch.await()
    
    endOfJob("all threads are finished!")
  }

}