
package com.dchekh

import java.util.Properties
import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.io.File

import org.apache.avro.Schema

import scala.collection.mutable.HashMap
import scala.xml.XML

object CFactory {

  val usage = """
Usage: parser [-v] [-f file] [-s sopt] ...
Where: -v   Run verbosely
       -f F Set input file to F
       -s S Set Show option to S
"""

  var schema_list: HashMap[String, Schema] = null // = SchemaListObj.list

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
    // if there are required args:
    //    if (args.length == 0) die()
    val arglist = args.toList
    val remainingopts = parseArgs(arglist, pf)

    if (filename.length == 0) filename = "d:/Users/Dzmitry_Chekh/Scala_workspace/CFactory/bin/consumer_groups.xml"

    println("CFactory v0.1")
    println("debug=" + debug)
    println("showme=" + showme)
    println("filename=" + filename)
    println("remainingopts=" + remainingopts)

    var cfg_XML = XML.loadFile(filename)

    schema_list = SchemaListObj.getSchemaList(cfg_XML)

    val latch = new CountDownLatch(SchemaListObj.getThread_number(cfg_XML))

    //---------- run consumer group -----------------// 
    val groupList = SchemaListObj.getcons_groupList(cfg_XML)
    groupList.foreach { n =>
      val i = n.asInstanceOf[Map[String, Any]]
      val cg = new ConsumerGroup(i("thread_number").toString().toInt, i("zkconnect").toString(), i("groupId").toString(), i("topic").toString(), latch).launch
      //println(s"$groupId, $zkconnect, $topic")
    }
    latch.await()

    endOfJob("all threads are finished!")
  }

}