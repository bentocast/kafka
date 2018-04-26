package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.locks.ReentrantLock

import kafka.utils.CoreUtils.inLock
import org.apache.log4j.Logger

object ClientAggregationController {
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  private val controllerLock: ReentrantLock = new ReentrantLock()
  final val DEFAULT_PERIOD = 600

  @volatile private var isEnabled = false
  var granularity = DEFAULT_PERIOD

  var cPool: ClientRequestConsumer = null
  var cScheduler: ClientRequestAggregatorScheduler = null

  def getEnable(): Boolean ={
      isEnabled
  }

  def setEnable(t: Boolean) {
    inLock(ClientAggregationController.controllerLock) {
      isEnabled = t
    }
  }

  def getGranularityInSec() = {
    granularity
  }

  def setGranularityInSec(v: Int) = {
    granularity = v
  }

  def start() {
    inLock(ClientAggregationController.controllerLock) {
      if(!isEnabled) {
        //TODO clear all collections before start
        ClientRequestFormatAppender.clearIncomingQeue()
        ClientAggregatorSet.clearAggregationSet()

        cPool = new ClientRequestConsumer()
        cPool.run()
        cScheduler = new ClientRequestAggregatorScheduler(granularity)
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