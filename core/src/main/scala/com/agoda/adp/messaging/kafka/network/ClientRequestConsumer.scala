package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.{ExecutorService, Executors}

import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{FetchRequest, OffsetCommitRequest, ProduceRequest}
import org.apache.log4j.Logger

import scala.collection.mutable

class ClientRequestConsumer(poolSize: Int) extends Runnable{
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  private val pool: ExecutorService = Executors.newFixedThreadPool(poolSize)
  private var isRunning = false
  var aggSet = new mutable.HashSet[String]()

//  private var Tricker = {
//    val scheduler = new KafkaScheduler(threads = 100, threadNamePrefix = "Log-MoveToSet-")
//    scheduler.startup()
//    scheduler.schedule("MoveToSet" , processData, 10, 10, unit = TimeUnit.MILLISECONDS)
//    headerExtractedInfo.trace("MoveToSet is set to " + 500.toString)
//    scheduler
//  }

  def run() = {
    try{
      if(!isRunning)
        while(true){
          if(headerExtractedInfo.isTraceEnabled){
            pool.execute(new HandlerProcessData(aggSet))
            isRunning = true
          }
        }
    } finally {
      pool.shutdown()
    }
  }
}

class HandlerProcessData(aggSet: mutable.HashSet[String]) extends Runnable {
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  private var count: Long = 0

  override def run() = {
    count+=1
    if (count%100 == 1) {headerExtractedInfo.trace(ClientRequestAggregator.consumerHeaderInfos.size())}
    //processData()
  }

  private def processData() {
    if (headerExtractedInfo.isTraceEnabled) {
      try {
        headerExtractedInfo.debug("ENTER")
        if (!ClientRequestAggregator.consumerHeaderInfos.isEmpty){
          headerExtractedInfo.debug("Size: " + ClientRequestAggregator.consumerHeaderInfos.size() + ",Take: 1")
          aggSet.add(formatData(ClientRequestAggregator.consumerHeaderInfos.take()))
        } else {
          headerExtractedInfo.debug("NO DATA EXISTS")
        }
      } catch {
        case iex: InterruptedException => headerExtractedInfo.debug(iex.printStackTrace())
        case e: Exception => headerExtractedInfo.debug(e.printStackTrace())
      }
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

object ClientRequestConsumer {
  private var instance:ClientRequestConsumer = null

  def getInstance(): ClientRequestConsumer ={
    if(instance == null) {
      instance = new ClientRequestConsumer(3)
    }
    instance
  }
}

