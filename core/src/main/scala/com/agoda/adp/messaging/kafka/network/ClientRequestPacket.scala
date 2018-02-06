package com.agoda.adp.messaging.kafka.network

import org.apache.kafka.common.requests.{AbstractRequest, RequestHeader}

class ClientRequestPacket(h: RequestHeader, b: AbstractRequest, c: String) {
  val header: RequestHeader = h
  val body: AbstractRequest = b
  val connectionId: String = c
}
