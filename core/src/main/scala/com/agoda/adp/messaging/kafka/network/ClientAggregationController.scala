package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.locks.ReentrantLock

import kafka.utils.CoreUtils.inLock

object ClientAggregationController {
  private val controllerLock: ReentrantLock = new ReentrantLock()
  final val DEFAULT_THREAD = 3
  final val DEFAULT_PERIOD = 10
  private var isEnabled = false
  var numberOfThread = DEFAULT_THREAD
  var printTraceLogPeriod = DEFAULT_PERIOD

  var cPool: ClientRequestConsumerPool = null
  var cScheduler: ClientRequestAggregatorScheduler = null

  def getEnable(): Boolean ={
//    inLock(ClientAggregationController.controllerLock) {
      isEnabled
//    }
  }

  def setEnable(t: Boolean) {
    inLock(ClientAggregationController.controllerLock) {
      isEnabled = t
    }
  }

  def start() {
    inLock(ClientAggregationController.controllerLock) {
      if(!isEnabled) {
        //TODO clear all queues !!!
        ClientRequestFormatAppender.clearIncomingQeue()
        ClientAggregatorSet.clearAggregationSet()

        cPool = new ClientRequestConsumerPool(numberOfThread)
        cPool.run()
        cScheduler = new ClientRequestAggregatorScheduler(printTraceLogPeriod)
        isEnabled = true
      }
    }
  }

  def stop(): Unit = {
    inLock(ClientAggregationController.controllerLock) {
      if (cPool != null) cPool.shutdown()
      if (cScheduler != null) cScheduler.shutdown()
      isEnabled = false
    }
  }

  def restart(){
    inLock(ClientAggregationController.controllerLock) {
      if (isEnabled) {
        stop()
        start()
      }
    }
  }
}