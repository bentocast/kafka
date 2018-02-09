package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


object ClientRequestFormatAppender {
  private val appenderLock: ReentrantLock = new ReentrantLock()
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  val headerInfoIncomingQueue = new ArrayBlockingQueue[String](100000)
  var overflowAggregationNum = new AtomicLong(0L)

  def appendIntoQueue(apiKey: Short, apiVersion: Short, clientId: String, topicPartitionSets: mutable.Set[TopicPartition], connectionId: String, groupId: String) = Future {
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
            ""
        }
      } else {
        ""
      }

      val topics = if(topicPartitionSets != null){
        val topicSets: mutable.Set[String] = new mutable.HashSet[String]()
        for( i <- topicPartitionSets){
          i match {
            case i: TopicPartition => topicSets.add(i.topic())
          }
        }
        mutable.SortedSet(topicSets.toList: _*).mkString(",")
      } else {
        ""
      }

      if (apiKey == ApiKeys.OFFSET_COMMIT.id) {

        val commitOffsetRecord = "{\"apiKey\":\"" + apiKey + "\"" +
          ",\"apiVersion\":\"" + apiVersion + "\"" +
          ",\"clientId\":\"" + clientId + "\"" +
          ",\"ip\":\"" + ip + "\"" +
          ",\"topics\":\"" + topics + "\""+
          ",\"consumerGroup\":\"" + groupId + "\"" +
          "}"
        if(!headerInfoIncomingQueue.offer(commitOffsetRecord)) overflowAggregationNum.incrementAndGet()
      } else {
        val produceORFetchRecord = "{\"apiKey\":\"" + apiKey + "\"" +
          ",\"apiVersion\":\"" + apiVersion + "\"" +
          ",\"clientId\":\"" + clientId + "\"" +
          ",\"ip\":\"" + ip + "\"" +
          ",\"topics\":\"" + topics + "\""+
          "}"
        if(!headerInfoIncomingQueue.offer(produceORFetchRecord)) overflowAggregationNum.incrementAndGet()
      }
    }
  }

  def clearIncomingQeue() {
    inLock(ClientRequestFormatAppender.appenderLock){
      headerInfoIncomingQueue.clear()
    }
  }
}
