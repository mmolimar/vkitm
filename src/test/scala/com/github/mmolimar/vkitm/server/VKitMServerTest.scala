package com.github.mmolimar.vkitm.server

import java.util.concurrent.TimeUnit

import com.github.mmolimar.vkitm.embedded.{EmbeddedKafkaCluster, EmbeddedVKitM, EmbeddedZookeeperServer}
import com.github.mmolimar.vkitm.utils.TestUtils
import kafka.common.Topic
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.{Node, TopicPartition}
import org.junit.runner.RunWith
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, WordSpec}

import scala.collection.JavaConverters._
import scala.collection.Seq

@RunWith(classOf[JUnitRunner])
class VKitMServerTest extends WordSpec with BeforeAndAfterAll with BeforeAndAfterEach {

  val zkServer = new EmbeddedZookeeperServer
  val kafkaCluster = new EmbeddedKafkaCluster(zkServer.getConnection)
  val vkitmServer = new EmbeddedVKitM(zkServer.getConnection, kafkaCluster.getBrokerList)

  val kafkaProducer = TestUtils.buildProducer(kafkaCluster.getBrokerList)
  val kafkaConsumer = TestUtils.buildConsumer(kafkaCluster.getBrokerList)

  val vkitmProducer = TestUtils.buildProducer(vkitmServer.getBrokerList)
  val vkitmConsumer = TestUtils.buildConsumer(vkitmServer.getBrokerList)

  var currentTopic: String = null

  "A VKitM producer" when {
    "producing" should {
      val key = "test-key".getBytes
      val value = "test-value".getBytes

      "create a topic and publish the message if the topic doesn't exist" in {
        val record = new ProducerRecord[Array[Byte], Array[Byte]](currentTopic, key, value)
        kafkaCluster.existTopic(currentTopic) should be(false)
        val metadata = vkitmProducer.send(record).get(1000, TimeUnit.MILLISECONDS)
        metadata.offset should be(0)
        kafkaCluster.existTopic(currentTopic) should be(true)
      }

      "increment the offset" in {
        val numMessages = 10
        kafkaCluster.existTopic(currentTopic) should be(false)
        val record = new ProducerRecord[Array[Byte], Array[Byte]](currentTopic, key, value)
        for (i <- 1 to numMessages) {
          val metadata = vkitmProducer.send(record).get(1000, TimeUnit.MILLISECONDS)
          metadata.offset should be(i - 1)
        }
        kafkaCluster.existTopic(currentTopic) should be(true)
      }

      "publish the same messages expected by the VKitM consumer" in {
        val numMessages = 10
        val record = new ProducerRecord[Array[Byte], Array[Byte]](currentTopic, key, value)

        kafkaCluster.createTopic(currentTopic)
        kafkaCluster.existTopic(currentTopic) should be(true)

        for (i <- 1 to numMessages) {
          val metadata = vkitmProducer.send(record).get(1000, TimeUnit.MILLISECONDS)
          metadata.offset should be(i - 1)
        }

        vkitmConsumer.subscribe(Seq(currentTopic).asJava, consumerRebalanceListener(vkitmConsumer))
        vkitmConsumer.subscription().contains(currentTopic) should be(true)

        val records: ConsumerRecords[Array[Byte], Array[Byte]] = vkitmConsumer.poll(1000)
        records.count should be(numMessages)
        records.asScala.foreach { record =>
          record.topic should be eq currentTopic
          record.key should be equals key
          record.value should be equals value
        }
        vkitmConsumer.unsubscribe
      }

      "publish the same messages expected by the Kafka consumer" in {
        val numMessages = 10
        val record = new ProducerRecord[Array[Byte], Array[Byte]](currentTopic, key, value)

        kafkaCluster.createTopic(currentTopic)
        kafkaCluster.existTopic(currentTopic) should be(true)

        for (i <- 1 to numMessages) {
          val metadata = vkitmProducer.send(record).get(1000, TimeUnit.MILLISECONDS)
          metadata.offset should be(i - 1)
        }

        kafkaConsumer.subscribe(Seq(currentTopic).asJava, consumerRebalanceListener(kafkaConsumer))
        kafkaConsumer.subscription().contains(currentTopic) should be(true)

        val records: ConsumerRecords[Array[Byte], Array[Byte]] = kafkaConsumer.poll(1000)
        records.count should be(numMessages)
        records.asScala.foreach { record =>
          record.topic should be eq currentTopic
          record.key should be equals key
          record.value should be equals value
        }
        kafkaConsumer.unsubscribe
      }
    }
  }

  "A Kafka producer" when {
    "producing" should {
      val key = "test-key".getBytes
      val value = "test-value".getBytes

      "publish the same messages expected by the VKitM consumer" in {
        val numMessages = 10
        val record = new ProducerRecord[Array[Byte], Array[Byte]](currentTopic, key, value)

        kafkaCluster.createTopic(currentTopic)
        kafkaCluster.existTopic(currentTopic) should be(true)

        for (i <- 1 to numMessages) {
          val metadata = vkitmProducer.send(record).get(1000, TimeUnit.MILLISECONDS)
          metadata.offset should be(i - 1)
        }

        vkitmConsumer.subscribe(Seq(currentTopic).asJava, consumerRebalanceListener(vkitmConsumer))
        vkitmConsumer.subscription().contains(currentTopic) should be(true)

        val records: ConsumerRecords[Array[Byte], Array[Byte]] = vkitmConsumer.poll(1000)
        records.count should be(numMessages)
        records.asScala.foreach { record =>
          record.topic should be eq currentTopic
          record.key should be equals key
          record.value should be equals value
        }
        vkitmConsumer.unsubscribe
      }
    }
  }

  "An admin" when {
    "listing topics" should {
      "expect that there are the same topics in VKitM and ZK" in {
        val topics = kafkaCluster.listTopics
        topics.isEmpty should be(false)
        topics.size should be(vkitmServer.getServer.metadataCache.getAllTopics.size)
        topics.foreach { topic =>
          vkitmServer.getServer.metadataCache.contains(topic) should be(true)
        }
      }
    }

    "creating topics" should {
      "expect the metadata in VKitM server is updated" in {
        val numPartitions = 25
        kafkaCluster.existTopic(currentTopic) should be(false)
        kafkaCluster.createTopic(currentTopic, numPartitions)
        kafkaCluster.existTopic(currentTopic) should be(true)
        for (i <- 1 until 5 if !vkitmServer.getServer.metadataCache.contains(currentTopic)) {
          //must sleep to wait the listener receives the update
          Thread.sleep(200)
        }
        vkitmServer.getServer.metadataCache.contains(currentTopic) should be(true)

        val metadata: Seq[MetadataResponse.TopicMetadata] = vkitmServer.getServer.metadataCache.getTopicMetadata(Set(currentTopic), SecurityProtocol.PLAINTEXT)
        metadata.isEmpty should be(false)
        metadata.foreach { tm =>
          tm.isInternal should be(false)
          tm.error.code should be(Errors.NONE.code)

          tm.partitionMetadata.size should be(1)
          tm.partitionMetadata.asScala.foreach { pm =>
            validateVirtualNode(pm.leader)
            pm.error.code should be(Errors.NONE.code)
            pm.isr.asScala.foreach(validateVirtualNode(_))
            pm.replicas.asScala.foreach(validateVirtualNode(_))
          }
        }
      }
    }

    "deleting topics" should {
      "expect the metadata in VKitM server is updated" in {
        val topics = kafkaCluster.listTopics
        topics.isEmpty should be(false)
        topics.filter(!Topic.isInternal(_)).foreach { topic => {
          vkitmServer.getServer.metadataCache.contains(topic) should be(true)
          kafkaCluster.deleteTopic(topic)
          for (i <- 1 until 5 if vkitmServer.getServer.metadataCache.contains(topic)) {
            //must sleep to wait the listener receives the update
            Thread.sleep(200)
          }
          vkitmServer.getServer.metadataCache.contains(topic) should be(false)
        }
        }
      }
    }
  }

  private def validateVirtualNode(node: Node) = {
    node.port should be(vkitmServer.getPort)
    node.id should be(VKitMServer.DEFAULT_VKITM_BROKER_ID)
  }

  private def checkTopicPartitions(topic: String) = {
    val vkitmTopicPartitions = vkitmConsumer.partitionsFor(topic).asScala
    val kafkaTopicPartitions = kafkaConsumer.partitionsFor(topic).asScala

    vkitmTopicPartitions.size should be(kafkaTopicPartitions.size)
    for ((tp, i) <- vkitmTopicPartitions.zipWithIndex) {
      tp.partition should be(kafkaTopicPartitions(i).partition)
      tp.topic should be equals (kafkaTopicPartitions(i).topic)

      validateVirtualNode(tp.leader())
      tp.inSyncReplicas.foreach(validateVirtualNode(_))
      tp.replicas.foreach(validateVirtualNode(_))
    }
  }

  private def consumerRebalanceListener[K, V](consumer: KafkaConsumer[K, V]) = new ConsumerRebalanceListener {
    override def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]) = {}

    override def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]) {
      for (partition <- partitions.asScala) consumer.seek(partition, 0)
    }
  }

  override def beforeEach() = currentTopic = TestUtils.randomString()

  override def afterEach() = checkTopicPartitions(currentTopic)

  override def beforeAll {
    zkServer.startup
    TestUtils.waitTillAvailable("localhost", zkServer.getPort, 5000)

    kafkaCluster.startup
    kafkaCluster.getPorts.foreach { port =>
      TestUtils.waitTillAvailable("localhost", port, 5000)
    }
    vkitmServer.startup
    TestUtils.waitTillAvailable("localhost", vkitmServer.getPort, 5000)

  }

  override def afterAll {
    vkitmServer.shutdown
    kafkaCluster.shutdown
    zkServer.shutdown
  }

}
