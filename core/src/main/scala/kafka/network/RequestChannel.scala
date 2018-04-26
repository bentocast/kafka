/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{TimeUnit, _}

import com.agoda.adp.messaging.kafka.network.ClientRequestFormatAppender
import com.yammer.metrics.core.Gauge
import kafka.api.{ControlledShutdownRequest, RequestOrResponse}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.QuotaId
import kafka.utils.{Logging, NotNothing}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.network.{ListenerName, Send}
import org.apache.kafka.common.protocol.{ApiKeys, Protocol, SecurityProtocol}
import org.apache.kafka.common.record.{RecordBatch, MemoryRecords}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Time
import org.apache.log4j.Logger

import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import scala.collection.mutable

object RequestChannel extends Logging {
  val AllDone = Request(processor = 1, connectionId = "2", Session(KafkaPrincipal.ANONYMOUS, InetAddress.getLocalHost),
    buffer = shutdownReceive, startTimeNanos = 0, listenerName = new ListenerName(""),
    securityProtocol = SecurityProtocol.PLAINTEXT)
  private val requestLogger = Logger.getLogger("kafka.request.logger")
  private val headerExtractedInfo = Logger.getLogger("kafka.headerinfo.logger")

  private def shutdownReceive: ByteBuffer = {
    val emptyProduceRequest = new ProduceRequest.Builder(RecordBatch.CURRENT_MAGIC_VALUE, 0, 0,
      Collections.emptyMap[TopicPartition, MemoryRecords]).build()
    val emptyRequestHeader = new RequestHeader(ApiKeys.PRODUCE.id, emptyProduceRequest.version, "", 0)
    emptyProduceRequest.serialize(emptyRequestHeader)
  }

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress) {
    val sanitizedUser = QuotaId.sanitize(principal.getName)
  }

  case class Request(processor: Int, connectionId: String, session: Session, private var buffer: ByteBuffer,
                     startTimeNanos: Long, listenerName: ListenerName, securityProtocol: SecurityProtocol) {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    @volatile var requestDequeueTimeNanos = -1L
    @volatile var apiLocalCompleteTimeNanos = -1L
    @volatile var responseCompleteTimeNanos = -1L
    @volatile var responseDequeueTimeNanos = -1L
    @volatile var apiRemoteCompleteTimeNanos = -1L
    @volatile var recordNetworkThreadTimeCallback: Option[Long => Unit] = None

    val requestId = buffer.getShort()

    // TODO: this will be removed once we remove support for v0 of ControlledShutdownRequest (which
    // depends on a non-standard request header)
    val requestObj: RequestOrResponse = if (requestId == ApiKeys.CONTROLLED_SHUTDOWN_KEY.id)
      ControlledShutdownRequest.readFrom(buffer)
    else
      null

    // if we failed to find a server-side mapping, then try using the
    // client-side request / response format
    val header: RequestHeader =
      if (requestObj == null) {
        buffer.rewind
        try RequestHeader.parse(buffer)
        catch {
          case ex: Throwable =>
            throw new InvalidRequestException(s"Error parsing request header. Our best guess of the apiKey is: $requestId", ex)
        }
      } else
        null
    val bodyAndSize: RequestAndSize =
      if (requestObj == null)
        try {
          // For unsupported version of ApiVersionsRequest, create a dummy request to enable an error response to be returned later
          if (header.apiKey == ApiKeys.API_VERSIONS.id && !Protocol.apiVersionSupported(header.apiKey, header.apiVersion)) {
            new RequestAndSize(new ApiVersionsRequest.Builder().build(), 0)
          }
          else
            AbstractRequest.getRequest(header.apiKey, header.apiVersion, buffer)
        } catch {
          case ex: Throwable =>
            throw new InvalidRequestException(s"Error getting request for apiKey: ${header.apiKey} and apiVersion: ${header.apiVersion}", ex)
        }
      else
        null

    buffer = null

    def requestDesc(details: Boolean): String = {
      if (requestObj != null)
        requestObj.describe(details)
      else
        s"$header -- ${body[AbstractRequest].toString(details)}"
    }

    def body[T <: AbstractRequest](implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
      bodyAndSize.request match {
        case r: T => r
        case r =>
          throw new ClassCastException(s"Expected request with type ${classTag.runtimeClass}, but found ${r.getClass}")
      }
    }

    trace("Processor %d received request : %s".format(processor, requestDesc(true)))

    def requestThreadTimeNanos = {
      if (apiLocalCompleteTimeNanos == -1L) apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds
      math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L)
    }

    //TODO if there is any needed request, save topicPartitionSets and groupId
    val topicPartitionSets: mutable.Set[TopicPartition] = if (header != null) {
      if (header.apiKey() == ApiKeys.OFFSET_COMMIT.id ||
        header.apiKey() == ApiKeys.FETCH.id ||
        header.apiKey() == ApiKeys.PRODUCE.id) {
        try {
          header.apiKey() match {
            case ApiKeys.OFFSET_COMMIT.id => body[OffsetCommitRequest].offsetData().keySet().asScala
            case ApiKeys.FETCH.id => body[FetchRequest].fetchData().keySet().asScala
            case ApiKeys.PRODUCE.id => body[ProduceRequest].partitionRecordsOrFail().keySet().asScala
          }
        } catch {
          case ex: Exception => headerExtractedInfo.debug("Could not extract Topics: Exception: " + ex.getMessage)
            null
        }
      } else {
        null
      }
    } else {
      null
    }

    val groupId: String = if (header != null) {
      if (header.apiKey == ApiKeys.OFFSET_COMMIT.id) {
        try {
          body[OffsetCommitRequest].groupId
        } catch {
          case ex: Exception => headerExtractedInfo.debug("Could not extract GroupId: Exception: " + ex.getMessage)
            "unknown"
        }
      } else {
        "unknown"
      }
    } else {
      "unknown"
    }

    def updateRequestMetrics(networkThreadTimeNanos: Long) {
      val endTimeNanos = Time.SYSTEM.nanoseconds
      // In some corner cases, apiLocalCompleteTimeNanos may not be set when the request completes if the remote
      // processing time is really small. This value is set in KafkaApis from a request handling thread.
      // This may be read in a network thread before the actual update happens in KafkaApis which will cause us to
      // see a negative value here. In that case, use responseCompleteTimeNanos as apiLocalCompleteTimeNanos.
      if (apiLocalCompleteTimeNanos < 0)
        apiLocalCompleteTimeNanos = responseCompleteTimeNanos
      // If the apiRemoteCompleteTimeNanos is not set (i.e., for requests that do not go through a purgatory), then it is
      // the same as responseCompleteTimeNans.
      if (apiRemoteCompleteTimeNanos < 0)
        apiRemoteCompleteTimeNanos = responseCompleteTimeNanos

      def nanosToMs(nanos: Long) = math.max(TimeUnit.NANOSECONDS.toMillis(nanos), 0)

      val requestQueueTime = nanosToMs(requestDequeueTimeNanos - startTimeNanos)
      val apiLocalTime = nanosToMs(apiLocalCompleteTimeNanos - requestDequeueTimeNanos)
      val apiRemoteTime = nanosToMs(apiRemoteCompleteTimeNanos - apiLocalCompleteTimeNanos)
      val apiThrottleTime = nanosToMs(responseCompleteTimeNanos - apiRemoteCompleteTimeNanos)
      val responseQueueTime = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos)
      val responseSendTime = nanosToMs(endTimeNanos - responseDequeueTimeNanos)
      val totalTime = nanosToMs(endTimeNanos - startTimeNanos)
      val fetchMetricNames =
        if (requestId == ApiKeys.FETCH.id) {
          val isFromFollower = body[FetchRequest].isFromFollower
          Seq(
            if (isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          )
        }
        else Seq.empty
      val metricNames = fetchMetricNames :+ ApiKeys.forId(requestId).name
      metricNames.foreach { metricName =>
        val m = RequestMetrics.metricsMap(metricName)
        m.requestRate.mark()
        m.requestQueueTimeHist.update(requestQueueTime)
        m.localTimeHist.update(apiLocalTime)
        m.remoteTimeHist.update(apiRemoteTime)
        m.throttleTimeHist.update(apiThrottleTime)
        m.responseQueueTimeHist.update(responseQueueTime)
        m.responseSendTimeHist.update(responseSendTime)
        m.totalTimeHist.update(totalTime)
      }

      // Records network handler thread usage. This is included towards the request quota for the
      // user/client. Throttling is only performed when request handler thread usage
      // is recorded, just before responses are queued for delivery.
      // The time recorded here is the time spent on the network thread for receiving this request
      // and sending the response. Note that for the first request on a connection, the time includes
      // the total time spent on authentication, which may be significant for SASL/SSL.
      recordNetworkThreadTimeCallback.foreach(record => record(networkThreadTimeNanos))

      if (requestLogger.isDebugEnabled) {
        val detailsEnabled = requestLogger.isTraceEnabled
        def nanosToMs(nanos: Long) = TimeUnit.NANOSECONDS.toMicros(math.max(nanos, 0)).toDouble / TimeUnit.MILLISECONDS.toMicros(1)
        val totalTimeMs = nanosToMs(endTimeNanos - startTimeNanos)
        val requestQueueTimeMs = nanosToMs(requestDequeueTimeNanos - startTimeNanos)
        val apiLocalTimeMs = nanosToMs(apiLocalCompleteTimeNanos - requestDequeueTimeNanos)
        val apiRemoteTimeMs = nanosToMs(apiRemoteCompleteTimeNanos - apiLocalCompleteTimeNanos)
        val apiThrottleTimeMs = nanosToMs(responseCompleteTimeNanos - apiRemoteCompleteTimeNanos)
        val responseQueueTimeMs = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos)
        val responseSendTimeMs = nanosToMs(endTimeNanos - responseDequeueTimeNanos)
        requestLogger.debug("Completed request:%s from connection %s;totalTime:%f,requestQueueTime:%f,localTime:%f,remoteTime:%f,throttleTime:%f,responseQueueTime:%f,sendTime:%f,securityProtocol:%s,principal:%s,listener:%s"
          .format(requestDesc(detailsEnabled), connectionId, totalTimeMs, requestQueueTimeMs, apiLocalTimeMs, apiRemoteTimeMs, apiThrottleTimeMs, responseQueueTimeMs, responseSendTimeMs, securityProtocol, session.principal, listenerName.value))
      }

      //TODO if there is any needed request, appendIntoQueue
      if (header != null) {
        if(header.apiKey() == ApiKeys.OFFSET_COMMIT.id ||
          header.apiKey() == ApiKeys.FETCH.id ||
          header.apiKey() == ApiKeys.PRODUCE.id) {

          val apiKey = header.apiKey()
          val apiVersion = header.apiVersion()
          val clientId = header.clientId()

          ClientRequestFormatAppender.appendIntoQueue(apiKey, apiVersion, clientId, topicPartitionSets, connectionId, groupId)
        }
      }
    }
  }

  object Response {

    def apply(request: Request, responseSend: Send): Response = {
      require(request != null, "request should be non null")
      require(responseSend != null, "responseSend should be non null")
      new Response(request, Some(responseSend), SendAction)
    }

    def apply(request: Request, response: AbstractResponse): Response = {
      require(request != null, "request should be non null")
      require(response != null, "response should be non null")
      apply(request, response.toSend(request.connectionId, request.header))
    }

  }

  case class Response(request: Request, responseSend: Option[Send], responseAction: ResponseAction) {
    request.responseCompleteTimeNanos = Time.SYSTEM.nanoseconds
    if (request.apiLocalCompleteTimeNanos == -1L) request.apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds

    def processor: Int = request.processor
  }

  trait ResponseAction
  case object SendAction extends ResponseAction
  case object NoOpAction extends ResponseAction
  case object CloseConnectionAction extends ResponseAction
}

class RequestChannel(val numProcessors: Int, val queueSize: Int) extends KafkaMetricsGroup {
  private var responseListeners: List[(Int) => Unit] = Nil
  private val requestQueue = new ArrayBlockingQueue[RequestChannel.Request](queueSize)
  private val responseQueues = new Array[BlockingQueue[RequestChannel.Response]](numProcessors)

  for(i <- 0 until numProcessors)
    responseQueues(i) = new LinkedBlockingQueue[RequestChannel.Response]()

  newGauge(
    "RequestQueueSize",
    new Gauge[Int] {
      def value = requestQueue.size
    }
  )

  newGauge( "overflowAggregationNum", new Gauge[AtomicLong] {
      def value = ClientRequestFormatAppender.overflowAggregationNum
    }
  )

  newGauge( "CurrentAggregationQueueSize", new Gauge[Long] {
    def value = ClientRequestFormatAppender.headerInfoIncomingQueue.size()
  }
  )

  newGauge("ResponseQueueSize", new Gauge[Int]{
    def value = responseQueues.foldLeft(0) {(total, q) => total + q.size()}
  })

  for (i <- 0 until numProcessors) {
    newGauge("ResponseQueueSize",
      new Gauge[Int] {
        def value = responseQueues(i).size()
      },
      Map("processor" -> i.toString)
    )
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request) {
    requestQueue.put(request)
  }

  /** Send a response back to the socket server to be sent over the network */
  def sendResponse(response: RequestChannel.Response) {
    responseQueues(response.processor).put(response)
    for(onResponse <- responseListeners)
      onResponse(response.processor)
  }

  /** Get the next request or block until specified time has elapsed */
  def receiveRequest(timeout: Long): RequestChannel.Request =
    requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /** Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.Request =
    requestQueue.take()

  /** Get a response for the given processor if there is one */
  def receiveResponse(processor: Int): RequestChannel.Response = {
    val response = responseQueues(processor).poll()
    if (response != null)
      response.request.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds
    response
  }

  def addResponseListener(onResponse: Int => Unit) {
    responseListeners ::= onResponse
  }

  def shutdown() {
    requestQueue.clear()
  }
}

object RequestMetrics {
  val metricsMap = new scala.collection.mutable.HashMap[String, RequestMetrics]
  val consumerFetchMetricName = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName = ApiKeys.FETCH.name + "Follower"
  (ApiKeys.values().toList.map(e => e.name)
    ++ List(consumerFetchMetricName, followFetchMetricName)).foreach(name => metricsMap.put(name, new RequestMetrics(name)))
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {
  val tags = Map("request" -> name)
  val requestRate = newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags)
  // time a request spent in a request queue
  val requestQueueTimeHist = newHistogram("RequestQueueTimeMs", biased = true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = newHistogram("LocalTimeMs", biased = true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram("RemoteTimeMs", biased = true, tags)
  // time a request is throttled (only relevant to fetch and produce requests)
  val throttleTimeHist = newHistogram("ThrottleTimeMs", biased = true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist = newHistogram("ResponseQueueTimeMs", biased = true, tags)
  // time to send the response to the requester
  val responseSendTimeHist = newHistogram("ResponseSendTimeMs", biased = true, tags)
  val totalTimeHist = newHistogram("TotalTimeMs", biased = true, tags)
}
