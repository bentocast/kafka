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

package kafka.integration

import kafka.common.TopicAndPartition
import kafka.utils.{Logging, ZKGroupTopicDirs}
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, ConsumerTimeoutException}
import kafka.server._
import kafka.utils.TestUtils
import kafka.serializer._
import kafka.producer.{KeyedMessage, Producer}
import org.junit.{After, Before, Test}
import org.apache.log4j.{Level, Logger}
import org.junit.Assert._

class AutoOffsetResetTest extends KafkaServerTestHarness with Logging {

  def generateConfigs() = List(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))

  val topic = "test_topic"
  val group = "default_group"
  val testConsumer = "consumer"
  val NumMessages = 10
  val LargeOffset = 10000
  val SmallOffset = -1
  
  val requestHandlerLogger = Logger.getLogger(classOf[kafka.server.KafkaRequestHandler])

  @Before
  override def setUp() {
    super.setUp()
    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)
  }

  @After
  override def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
    super.tearDown
  }

  @Test
  def testResetToEarliestWhenOffsetTooHigh() =
    assertEquals(NumMessages, resetAndConsume(NumMessages, "smallest", LargeOffset, 0))

  @Test
  def testResetToEarliestWhenOffsetTooLow() =
    assertEquals(NumMessages, resetAndConsume(NumMessages, "smallest", SmallOffset, 0))

  @Test
  def testResetToLatestWhenOffsetTooHigh() =
    assertEquals(0, resetAndConsume(NumMessages, "largest", LargeOffset, 0))

  @Test
  def testResetToLatestWhenOffsetTooLow() =
    assertEquals(0, resetAndConsume(NumMessages, "largest", SmallOffset, 0))

  @Test
  def testSmartResetWhenReqOffsetTooLowAndStrategySmallest() =
    assertEquals(NumMessages, resetAndConsume(NumMessages, "smallest", 5, 10, ("adp.offset.smart.reset", "true")))

  @Test
  def testSmartResetWhenReqOffsetTooLowAndStrategyLargest() =
    assertEquals(NumMessages, resetAndConsume(NumMessages, "largest", 5, 10, ("adp.offset.smart.reset", "true")))

  @Test
  def testSmartResetWhenReqOffsetTooHighAndStrategySmallest() =
    assertEquals(0, resetAndConsume(NumMessages, "smallest", 100, 10, ("adp.offset.smart.reset", "true")))

  @Test
  def testSmartResetWhenReqOffsetTooHighAndStrategyLargest() =
    assertEquals(0, resetAndConsume(NumMessages, "largest", 100, 10, ("adp.offset.smart.reset", "true")))

  /* Set the starting offset of a test topic to a given offset, produce the given number of messages,
   * create a consumer with the given offset policy and given offset, and consume until we get no new messages.
   * Returns the count of messages received.
   */
  def resetAndConsume(numMessages: Int, resetTo: String, consumerInitialOffset: Long, firstOffsetInServer: Long, extraConsumerProps: (String, AnyRef)*): Int = {
    TestUtils.createTopic(zkUtils, topic, 1, 1, servers)

    if (firstOffsetInServer != 0L)
      servers(0).logManager.truncateFullyAndStartAt(new TopicAndPartition(topic, 0), firstOffsetInServer);

    val producer: Producer[String, Array[Byte]] = TestUtils.createProducer(
      TestUtils.getBrokerListStrFromServers(servers),
      keyEncoder = classOf[StringEncoder].getName)

    for(i <- 0 until numMessages)
      producer.send(new KeyedMessage[String, Array[Byte]](topic, topic, "test".getBytes))

    // update offset in zookeeper for consumer to jump "forward" in time
    val dirs = new ZKGroupTopicDirs(group, topic)
    val consumerProps = TestUtils.createConsumerProperties(zkConnect, group, testConsumer)
    consumerProps.put("auto.offset.reset", resetTo)
    consumerProps.put("consumer.timeout.ms", "2000")
    consumerProps.put("fetch.wait.max.ms", "0")
    extraConsumerProps.foreach(p => consumerProps.put(p._1, p._2));
    val consumerConfig = new ConsumerConfig(consumerProps)

    TestUtils.updateConsumerOffset(consumerConfig, dirs.consumerOffsetDir + "/" + "0", consumerInitialOffset)
    info("Updated consumer offset to " + consumerInitialOffset)
    
    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val messageStream = consumerConnector.createMessageStreams(Map(topic -> 1))(topic).head

    var received = 0
    val iter = messageStream.iterator
    try {
      for (i <- 0 until numMessages) {
        iter.next // will throw a timeout exception if the message isn't there
        received += 1
      }
    } catch {
      case e: ConsumerTimeoutException => 
        info("consumer timed out after receiving " + received + " messages.")
    } finally {
      producer.close()
      consumerConnector.shutdown
    }
    received
  }
  
}
