package com.agoda.adp.messaging.kafka.network

import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{FetchRequest, OffsetCommitRequest, ProduceRequest}
import org.apache.kafka.common.utils.Utils
import org.apache.log4j.Logger

class ClientRequestConsumerPool(numThreads: Int) {
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  val threads = new Array[Thread](numThreads)
  val runnables = new Array[ProcessDataHandler](numThreads)

  for(i <- 0 until numThreads) {
    runnables(i) = new ProcessDataHandler()
    threads(i) = Utils.daemonThread("kafka-process-data-handler-" + i, runnables(i))
    threads(i).start()
  }

  def shutdown() {
    headerExtractedInfo.info("shutting down")
    for(handler <- runnables)
      // DO STH
      // handler.shutdown
    for(thread <- threads)
      thread.join
    headerExtractedInfo.info("shut down completely")
  }
}

class ProcessDataHandler() extends Runnable {
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  private var count: Long = 0

  override def run() = {
    try{
      while(true){
        processData()
      }
    } catch {
      case e: Throwable => headerExtractedInfo.error("Exception when handling request", e)
    }

  }

  private def processData() {
    try {
      if (!ClientFilterRequestAppender.consumerHeaderInfos.isEmpty) {
        ClientAggregatorSelector.getSetAggregator().add(formatData(ClientFilterRequestAppender.consumerHeaderInfos.take()))
      }
    } catch {
      case iex: InterruptedException => headerExtractedInfo.debug(iex.printStackTrace())
      case e: Exception => headerExtractedInfo.debug(e.printStackTrace())
    }
  }

  private def formatData(packet: ClientRequestPacket): String = {
    val header = packet.header
    val body = packet.body
    val connectionId = packet.connectionId

    val api_key = header.apiKey()
    val api_version = header.apiVersion()
    val client_id = header.clientId()
    val ip = connectionId.split("-")(1).split(":")(0)

    var topics = ""

    header.apiKey() match {
      case ApiKeys.OFFSET_COMMIT.id => topics = body.asInstanceOf[OffsetCommitRequest].offsetData().keySet().toArray.mkString(",")
      case ApiKeys.FETCH.id => topics = body.asInstanceOf[FetchRequest].fetchData().keySet().toArray().mkString(",")
      case ApiKeys.PRODUCE.id => topics = body.asInstanceOf[ProduceRequest].partitionRecordsOrFail().keySet().toArray().mkString(",")
    }

    if (api_key == ApiKeys.OFFSET_COMMIT.id) {
      val group_id = body.asInstanceOf[OffsetCommitRequest].groupId
      val commitOffsetRecord = "{ apiKey:" + api_key +
        ", apiVersion:" + api_version +
        ", clientId:" + client_id +
        ", ip:" + ip +
        ", topics:" + topics +
        ", consumerGroup: " + group_id +
        " }"
      commitOffsetRecord
    } else {
      val produceORFetchRecord = "{ apiKey:" + api_key +
        ", apiVersion:" + api_version +
        ", clientId:" + client_id +
        ", ip:" + ip +
        ", topics:" + topics +
        " }"
      produceORFetchRecord
    }
  }
}

object ClientRequestConsumerPool {
  private var instance: ClientRequestConsumerPool = new ClientRequestConsumerPool(1)
}

