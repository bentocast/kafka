package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.ArrayBlockingQueue

import org.apache.kafka.common.requests.{AbstractRequest, RequestHeader}
import org.apache.log4j.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


object ClientRequestAggregator {
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")

  val consumerHeaderInfos = new ArrayBlockingQueue[ClientRequestPacket](100000)
  val flagLock = false
  var overflowAggregationNum: Long = 0

  def clear(): Unit ={
    consumerHeaderInfos.clear()
  }

  def appendIntoQueue(header: RequestHeader, body: AbstractRequest, connectionId: String) = Future {
    val clientRequestPacket = new ClientRequestPacket(header, body, connectionId)
    val isSuccess = consumerHeaderInfos.offer(clientRequestPacket)
    if(!isSuccess){
      overflowAggregationNum+=1
      headerExtractedInfo.info("REJECTED 1")
    } else {
      headerExtractedInfo.info("RECEIVED 1")
    }
  }
}
