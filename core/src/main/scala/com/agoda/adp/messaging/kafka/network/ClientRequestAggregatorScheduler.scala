package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import kafka.utils.KafkaScheduler

class ClientRequestAggregatorScheduler(timeToSchedule: Int) {
  //TODO Use only 1 thread to write result LOG

  private val headerExtractedInfo = Logger("kafka.headerinfo.logger")
  val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "Log-PrintingRequestLogAggregation-")
  scheduler.startup()
  scheduler.schedule("PrintingRequestLogAggregation-" + timeToSchedule, printAggregationSetTask, timeToSchedule, timeToSchedule, unit = TimeUnit.SECONDS)

  def shutdown(){
    if(scheduler.isStarted){
      scheduler.shutdown()
    }
  }

  private def printAggregationSetTask() {
    if(ClientAggregationController.getEnable()) {
      headerExtractedInfo.debug("#timeToPrint: " + timeToSchedule.toString)
      val snapshot = ClientAggregatorSet.takeAggregationSet()
      if(snapshot.nonEmpty){
        headerExtractedInfo.debug("#total: " + snapshot.size)
        snapshot.foreach(rec => headerExtractedInfo.info(rec))
      }
    }
  }
}