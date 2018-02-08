package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests._
import org.apache.log4j.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


object ClientRequestFormatAppender {
  private val appenderLock: ReentrantLock = new ReentrantLock()
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  val headerInfoIncomingQueue = new ArrayBlockingQueue[String](100000)
  var overflowAggregationNum = new AtomicLong(0L)

  def appendIntoQueue(header: RequestHeader, body: AbstractRequest, connectionId: String) = Future {
    //TODO extract useful info before adding into Queue
    //TODO To be safe as much as possible
    //TODO atomic/volatile overflow count
    //TODO move check enableFlag to Future
    if (ClientAggregationController.getEnable()) {

      val api_key = header.apiKey()
      val api_version = header.apiVersion()
      val client_id = header.clientId()

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

      val topics: String = try {
        header.apiKey() match {
          case ApiKeys.OFFSET_COMMIT.id => body.asInstanceOf[OffsetCommitRequest].offsetData().keySet().toArray.mkString(",")
          case ApiKeys.FETCH.id => body.asInstanceOf[FetchRequest].fetchData().keySet().toArray().mkString(",")
          case ApiKeys.PRODUCE.id => body.asInstanceOf[ProduceRequest].partitionRecordsOrFail().keySet().toArray().mkString(",")
          case _ => {
            headerExtractedInfo.debug("Could not extract Topics: UNKNOWN apikey")
            ""
          }
        }
      } catch {
        case ex: Exception => headerExtractedInfo.debug("Could not extract Topics: Exception: " + ex.getMessage)
          ""
      }


      if (api_key == ApiKeys.OFFSET_COMMIT.id) {
        val group_id: String = try {
          body.asInstanceOf[OffsetCommitRequest].groupId
        } catch {
          case ex: Exception => headerExtractedInfo.debug("Could not extract GroupId: Exception: " + ex.getMessage)
            ""
        }

        val commitOffsetRecord = "{\"apiKey\":\"" + api_key + "\"" +
          ",\"apiVersion\":\"" + api_version + "\"" +
          ",\"clientId\":\"" + client_id + "\"" +
          ",\"ip\":\"" + ip + "\"" +
          ",\"topics\":\"" + topics + "\""+
          ",\"consumerGroup\":\"" + group_id + "\"" +
          "}"
        if(!headerInfoIncomingQueue.offer(commitOffsetRecord)) overflowAggregationNum.incrementAndGet()
      } else {
        val produceORFetchRecord = "{\"apiKey\":\"" + api_key + "\"" +
          ",\"apiVersion\":\"" + api_version + "\"" +
          ",\"clientId\":\"" + client_id + "\"" +
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
