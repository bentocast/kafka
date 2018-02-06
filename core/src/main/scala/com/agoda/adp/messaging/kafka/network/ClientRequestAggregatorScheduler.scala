package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.TimeUnit
import kafka.utils.KafkaScheduler
import org.apache.log4j.Logger

class ClientRequestAggregatorScheduler {
  final val DEFAULT_PERIOD = 5
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  var period = DEFAULT_PERIOD
  private var tempPeriod = period
  private var aggScheduler = {
    val scheduler = new KafkaScheduler(threads = 100, threadNamePrefix = "Log-Aggregation-")
    scheduler.startup()
    scheduler.schedule("ReqeustHeader-" + tempPeriod, printAggregationLogTask, tempPeriod, tempPeriod, unit = TimeUnit.SECONDS)
//    headerExtractedInfo.trace("Aggregation Period is set to " + tempPeriod.toString)
    scheduler
  }

  private def terminateScheduler(scheduler: KafkaScheduler){
    if(scheduler.isStarted){
      scheduler.shutdown()
    }
  }

  private def printAggregationLogTask() {
    if(headerExtractedInfo.isTraceEnabled) {
      headerExtractedInfo.trace("Aggregation Period is " + tempPeriod.toString)
//      if(!ClientRequestConsumer.instance.aggSet.isEmpty)
//        ClientRequestConsumer.instance.aggSet.foreach( rec => headerExtractedInfo.trace(rec))
    }
  }
}

object ClientRequestAggregatorScheduler {
  private val instance: ClientRequestAggregatorScheduler = new ClientRequestAggregatorScheduler()
}