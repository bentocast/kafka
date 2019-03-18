package com.agoda.adp.messaging.kafka.network

import java.util.concurrent.locks.ReentrantLock

import com.typesafe.scalalogging.Logger
import kafka.utils.CoreUtils.inLock

import scala.collection.mutable

object ClientAggregatorSet {
  private val headerExtractedInfo = Logger("kafka.headerinfo.logger")
  private val aggregatorLock: ReentrantLock = new ReentrantLock()

  @volatile var aggSet: mutable.HashSet[String] = new mutable.HashSet[String]()

  def takeAggregationSet(): mutable.HashSet[String] = {
    headerExtractedInfo.debug("ClientAggregatorSet STATUS: " + ClientAggregatorSet.aggregatorLock.isLocked.toString)
    inLock(ClientAggregatorSet.aggregatorLock) {
      headerExtractedInfo.debug("ClientAggregatorSet STATUS: " + ClientAggregatorSet.aggregatorLock.isLocked.toString + ", Previous aggSet Size : " + aggSet.size)
      val oldAggSet = aggSet
      aggSet = new mutable.HashSet[String]()
      oldAggSet
    }
  }

  def clearAggregationSet() {
    inLock(ClientAggregatorSet.aggregatorLock) {
      headerExtractedInfo.debug("ClientAggregatorSet STATUS: " + ClientAggregatorSet.aggregatorLock.isLocked.toString + ", Clear aggSet")
      aggSet.clear()
    }
  }
}