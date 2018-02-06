package com.agoda.adp.messaging.kafka.network

import org.apache.log4j.Logger

import scala.collection.mutable

object ClientAggregatorSelector {
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")
  var isTimeToSwitch = false
  var pointer = 0
  var aggSet: Array[mutable.HashSet[String]] = new Array[mutable.HashSet[String]](2)
  aggSet(0) = new mutable.HashSet[String]()
  aggSet(1) = new mutable.HashSet[String]()

  def getSetAggregator(): mutable.HashSet[String] = {
    if(isTimeToSwitch){
      val pre = pointer
      headerExtractedInfo.info("Time to Switch from : " + pointer + ", Size: " + aggSet(pointer).size)
      if(pointer == 0){
        pointer = 1
      } else {
        pointer = 0
      }
      headerExtractedInfo.info("Time to Switch to : " + pointer + ", Size: " + aggSet(pointer).size)
      isTimeToSwitch = false
      aggSet(pre).clear()
    }
    aggSet(pointer)
  }
}