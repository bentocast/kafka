package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.locks.ReentrantLock

import kafka.utils.CoreUtils.inLock
import org.apache.log4j.Logger

object ClientAggregationController {
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  private val controllerLock: ReentrantLock = new ReentrantLock()
  final val DEFAULT_THREAD = 1
  final val DEFAULT_PERIOD = 600
  @volatile private var isEnabled = false
  var numberOfThread = DEFAULT_THREAD
  var printTraceLogPeriod = DEFAULT_PERIOD

  var cPool: ClientRequestConsumerPool = null
  var cScheduler: ClientRequestAggregatorScheduler = null

  def getEnable(): Boolean ={
      isEnabled
  }

  def setEnable(t: Boolean) {
    inLock(ClientAggregationController.controllerLock) {
      isEnabled = t
    }
  }

  def start() {
    inLock(ClientAggregationController.controllerLock) {
      if(!isEnabled) {
        //TODO clear all collections before start
        ClientRequestFormatAppender.clearIncomingQeue()
        ClientAggregatorSet.clearAggregationSet()

        cPool = new ClientRequestConsumerPool(numberOfThread)
        cPool.run()
        cScheduler = new ClientRequestAggregatorScheduler(printTraceLogPeriod)
        isEnabled = true
        headerExtractedInfo.debug("Started ...")
      }
    }
  }

  def stop(): Unit = {
    inLock(ClientAggregationController.controllerLock) {
      if (cPool != null) cPool.shutdown()
      if (cScheduler != null) cScheduler.shutdown()

      //TODO Defensive check
      cPool = null
      cScheduler = null

      isEnabled = false
      headerExtractedInfo.debug("Shutdown Completed !!")
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