package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.locks.ReentrantLock

import kafka.utils.CoreUtils.inLock
import org.apache.log4j.Logger

import scala.collection.mutable

object ClientAggregatorSet {
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  private val aggregatorLock: ReentrantLock = new ReentrantLock()
  var aggSet: mutable.HashSet[String] = new mutable.HashSet[String]()

  def takeAggregationSet(): mutable.HashSet[String] = {
    headerExtractedInfo.debug("ClientAggregatorSet STATUS: " + ClientAggregatorSet.aggregatorLock.isLocked.toString)
    inLock(ClientAggregatorSet.aggregatorLock) {
      headerExtractedInfo.debug("ClientAggregatorSet STATUS: " + ClientAggregatorSet.aggregatorLock.isLocked.toString + ", Previous aggSet Size : " + aggSet.size)
      val oldagg = aggSet
      aggSet = new mutable.HashSet[String]()
      oldagg
    }
  }

  def clearAggregationSet() {
    inLock(ClientAggregatorSet.aggregatorLock) {
      headerExtractedInfo.debug("ClientAggregatorSet STATUS: " + ClientAggregatorSet.aggregatorLock.isLocked.toString + ", Clear aggSet")
      aggSet.clear()
    }
  }
}