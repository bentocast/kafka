package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import org.apache.log4j.Logger

class ClientRequestConsumerPool(numThreads: Int) {
  //TODO Excutor implemetation
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  private val pool: ExecutorService = Executors.newFixedThreadPool(numThreads)

  def run() {
    headerExtractedInfo.debug("Size of ThreadPool: " + numThreads)
    try {
      for(i <- 1 to numThreads){
        headerExtractedInfo.debug("Spawn #Thread: " + i)
        pool.execute(new processHandler())
      }
    } catch {
      case ex: Exception => {
        headerExtractedInfo.debug(ex.printStackTrace())
        shutdown()
      }
    }
  }

  def shutdown() {
    pool.shutdown()
    try {
      if(!pool.awaitTermination(10, TimeUnit.SECONDS)){
        pool.shutdownNow()
        if(!pool.awaitTermination(10, TimeUnit.SECONDS)){
          headerExtractedInfo.debug("ClientRequestConsumerPool did not terminate")
        }
      }
    } catch {
      case e: InterruptedException => {
        pool.shutdownNow()
        Thread.currentThread().interrupt()
      }
    }
  }
}

class processHandler() extends Runnable {
  override def run() = {
    while (true) {
      addIntoAggregationSet()
    }
  }

  private def addIntoAggregationSet() {
    try {
        ClientAggregatorSet.aggSet
          .add(ClientRequestFormatAppender.headerInfoIncomingQueue.take())
    } catch {
      //TODO Not print out
      case e: Exception =>
    }
  }
}

