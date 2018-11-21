package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import org.apache.log4j.Logger

class ClientRequestConsumer() {
  //TODO Excutor implemetation
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  private val pool: ExecutorService = Executors.newFixedThreadPool(1)
  private val processHandler = new processHandler()

  def run() {
    try {
        headerExtractedInfo.debug("Spawn #ConsumerThread")
        pool.execute(processHandler)
    } catch {
      case ex: Exception => {
        headerExtractedInfo.debug(ex.printStackTrace())
        shutdown()
      }
    }
  }

  def shutdown() {
    processHandler.shutdown()
    pool.shutdown()
    try {
      if(!pool.awaitTermination(10, TimeUnit.SECONDS)){
          headerExtractedInfo.debug("Awaiting completion of ClientRequestConsumerPool")
      }
      pool.shutdownNow()
      if(!pool.awaitTermination(10, TimeUnit.SECONDS)){
        headerExtractedInfo.debug("Pool did not terminate")
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
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  @volatile private var shuttingDown = false

  override def run() = {
    try{
      while (!shuttingDown) {
        addIntoAggregationSet()
      }
    } catch {
      case ex: InterruptedException => headerExtractedInfo.debug("ProcessHadler was shutdown")
    }
  }

  private def addIntoAggregationSet() {
    try {
      //Poll queue every secs instead of blocking on take() to make the thread responsive to shutting down flag
      ClientAggregatorSet.aggSet.add( ClientRequestFormatAppender.headerInfoIncomingQueue.poll(1 , TimeUnit.SECONDS) )
    } catch {
      case ex: InterruptedException => throw ex
      case e: Exception => headerExtractedInfo.error("Add into aggregation set found exception, e: " + e.getMessage)
    }
  }

  def shutdown(){
    shuttingDown = true
  }
}

