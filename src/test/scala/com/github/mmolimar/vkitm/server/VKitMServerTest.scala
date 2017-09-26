package com.github.mmolimar.vkitm.server

import java.util.concurrent.TimeUnit

import com.github.mmolimar.vkitm.embedded.{EmbeddedKafkaCluster, EmbeddedVKitM, EmbeddedZookeeperServer}
import com.github.mmolimar.vkitm.utils.TestUtils
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.internals.Topic
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
  val kafkaAdminClient = TestUtils.buildAdminClient(kafkaCluster.getBrokerList)

  val vkitmProducer = TestUtils.buildProducer(vkitmServer.getBrokerList)
  val vkitmConsumer = TestUtils.buildConsumer(vkitmServer.getBrokerList)
  val vkitmAdminClient = TestUtils.buildAdminClient(vkitmServer.getBrokerList)

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

        val records: ConsumerRecords[Array[Byte], Array[Byte]] = vkitmConsumer.poll(10000)
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

        val records: ConsumerRecords[Array[Byte], Array[Byte]] = kafkaConsumer.poll(10000)
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

        val records: ConsumerRecords[Array[Byte], Array[Byte]] = vkitmConsumer.poll(10000)
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

  "A VKitM admin client" when {

    "listing topics" should {
      "expect that there are the same topics in VKitM and ZK" in {
        vkitmAdminClient.createTopics(Seq(new NewTopic(currentTopic, 1, 1)).asJava).all.get

        val topics = kafkaCluster.listTopics

        topics.isEmpty should be(false)
        topics.size should be(vkitmServer.getServer.metadataCache.getAllTopics.size)
        topics.foreach { topic =>
          vkitmServer.getServer.metadataCache.contains(topic) should be(true)
        }
      }

      "expect that the topics are the same as the returned by the Kafka admin client" in {
        vkitmAdminClient.createTopics(Seq(new NewTopic(currentTopic, 1, 1)).asJava).all.get

        val kafkaTopics = kafkaAdminClient.listTopics.listings.get.asScala.map(tl => tl.name -> tl.isInternal).toMap
        val vkitmTopics = vkitmAdminClient.listTopics.listings.get.asScala.map(tl => tl.name -> tl.isInternal).toMap

        vkitmTopics.isEmpty should be(false)
        kafkaTopics.isEmpty should be(false)
        vkitmTopics.size should be(kafkaTopics.size)

        vkitmTopics.foreach(tl => {
          kafkaTopics.get(tl._1).isEmpty should be(false)
          kafkaTopics.get(tl._1).get should be(tl._2)
        })
      }
    }

    "creating topics" should {
      "expect the metadata in VKitM server is updated" in {
        val numPartitions = 25

        vkitmAdminClient.listTopics.names.get.contains(currentTopic) should be(false)
        val created = vkitmAdminClient.createTopics(Seq(new NewTopic(currentTopic, numPartitions, 1)).asJava).all.get
        created should be(null)

        vkitmAdminClient.listTopics.names.get.contains(currentTopic) should be(true)
        kafkaAdminClient.listTopics.names.get.contains(currentTopic) should be(true)
        vkitmServer.getServer.metadataCache.contains(currentTopic) should be(true)

        val metadata: Seq[MetadataResponse.TopicMetadata] = vkitmServer.getServer.metadataCache.getTopicMetadata(Set(currentTopic), SecurityProtocol.PLAINTEXT)
        metadata.isEmpty should be(false)
        metadata.foreach { tm =>
          tm.isInternal should be(false)
          tm.error.code should be(Errors.NONE.code)

          tm.partitionMetadata.size should be(numPartitions)
          tm.partitionMetadata.asScala.foreach { pm =>
            validateVirtualNode(pm.leader)
            pm.error.code should be(Errors.NONE.code)
            pm.isr.asScala.foreach(validateVirtualNode(_))
            pm.replicas.asScala.foreach(validateVirtualNode(_))
          }
        }
      }
    }

    "describing topics" should {
      "expect the same description as returned from the Kafka admin client" in {
        val numPartitions = 25
        vkitmAdminClient.createTopics(Seq(new NewTopic(currentTopic, numPartitions, 1)).asJava)
        vkitmAdminClient.listTopics.names.get.contains(currentTopic) should be(true)
        kafkaAdminClient.listTopics.names.get.contains(currentTopic) should be(true)

        val vkitmTopicDescription = vkitmAdminClient.describeTopics(Seq(currentTopic).asJava).all.get.asScala
        val kafkaTopicDescription = kafkaAdminClient.describeTopics(Seq(currentTopic).asJava).all.get.asScala

        vkitmTopicDescription.isEmpty should be(false)
        kafkaTopicDescription.isEmpty should be(false)

        vkitmTopicDescription.foreach(record => {
          val vkitmTd = record._2
          val kafkaTd = kafkaTopicDescription.get(record._1).get

          kafkaTd shouldNot be(null)
          vkitmTd.isInternal should be(kafkaTd.isInternal)
          vkitmTd.name should be(kafkaTd.name)
          vkitmTd.partitions.size should be(numPartitions)
          kafkaTd.partitions.size should be(numPartitions)
          vkitmTd.partitions.asScala.foreach(pi => {
            validateVirtualNode(pi.leader)
            pi.isr.asScala.foreach(validateVirtualNode(_))
            pi.replicas.asScala.foreach(validateVirtualNode(_))
          })
        })
      }
    }

    "deleting topics" should {
      "expect the metadata in VKitM server is updated" in {
        vkitmAdminClient.createTopics(Seq(new NewTopic(currentTopic, 1, 1)).asJava)
        val topics = vkitmAdminClient.listTopics.names.get

        topics.isEmpty should be(false)
        topics.asScala.filter(!Topic.isInternal(_)).foreach(topic => {

          vkitmServer.getServer.metadataCache.contains(topic) should be(true)

          vkitmAdminClient.deleteTopics(Seq(topic).asJava).all.get

          vkitmServer.getServer.metadataCache.contains(topic) should be(false)
          vkitmAdminClient.listTopics.names.get.contains(topic) should be(false)
        }
        )
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
    for (vkitmTp <- vkitmTopicPartitions) {
      kafkaTopicPartitions.map(kafkaTp => {
        vkitmTp.topic should be equals (kafkaTp.topic)
        kafkaTp.partition
      }).contains(vkitmTp.partition) should be(true)

      validateVirtualNode(vkitmTp.leader())
      vkitmTp.inSyncReplicas.foreach(validateVirtualNode(_))
      vkitmTp.replicas.foreach(validateVirtualNode(_))
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
    kafkaAdminClient.close
    vkitmAdminClient.close

    kafkaProducer.close
    vkitmProducer.close

    kafkaConsumer.close
    vkitmConsumer.close

    vkitmServer.shutdown
    kafkaCluster.shutdown
    zkServer.shutdown
  }

}
