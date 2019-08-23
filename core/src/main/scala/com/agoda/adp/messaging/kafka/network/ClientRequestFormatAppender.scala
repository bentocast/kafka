package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

import com.typesafe.scalalogging.Logger

import scala.collection.Set
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


object ClientRequestFormatAppender {
  private val appenderLock: ReentrantLock = new ReentrantLock()
  private val headerExtractedInfo = Logger("kafka.headerinfo.logger")
  val headerInfoIncomingQueue = new ArrayBlockingQueue[String](100000)
  var overflowAggregationNum = new AtomicLong(0L)

  def appendIntoQueue(apiKey: Short, apiVersion: Short, clientId: String, topicPartitionSets: Set[String], connectionId: String, groupId: String) = Future {
    //TODO extract useful info before adding into headerInfoIncomingQueue
    //TODO To be safe as much as possible
    //TODO atomic/volatile overflow count
    //TODO move check isEnable flag to Future scope

    if (ClientAggregationController.getEnable()) {
      val ip: String = if (connectionId.nonEmpty) {
        try {
          connectionId.split("-")(1).split(":")(0)
        } catch {
          case ex: Exception => headerExtractedInfo.debug("Could not extract IP from 'connectionId' Exception: " + ex.getMessage)
            "unknown"
        }
      } else {
        "unknown"
      }

      val topics = if (topicPartitionSets != null) {
        topicPartitionSets.mkString(",")
      } else {
        "unknown"
      }

      val record = "{\"apiKey\":\"" + apiKey + "\"" +
        ",\"apiVersion\":\"" + apiVersion + "\"" +
        ",\"clientId\":\"" + clientId + "\"" +
        ",\"ip\":\"" + ip + "\"" +
        ",\"topics\":\"" + topics + "\"" + {
        if (!groupId.equals("unknown")) ",\"consumerGroup\":\"" + groupId + "\"" else ""
      } +
        "}"
      if (!headerInfoIncomingQueue.offer(record)) overflowAggregationNum.incrementAndGet()
    }
  }

  def clearIncomingQeue() {
    headerInfoIncomingQueue.clear()
  }
}
