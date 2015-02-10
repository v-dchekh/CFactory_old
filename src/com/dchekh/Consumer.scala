package com.dchekh

import java.util.concurrent.CountDownLatch;

  class Consumer[T](mes: String, sleeptime: Int, cdl : CountDownLatch ) extends Runnable {
    def run() {
      Thread.sleep(sleeptime)
      println(mes)
      cdl.countDown()

    }
  }
