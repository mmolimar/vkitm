package integration.com.github.mmolimar.vkitm

import java.util.concurrent.TimeUnit

import integration.com.github.mmolimar.vkitm.embedded.{EmbeddedKafkaCluster, EmbeddedVKitM, EmbeddedZookeeperServer}
import integration.com.github.mmolimar.vkitm.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.runner.RunWith
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, WordSpec}

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class VKitMTest extends WordSpec with BeforeAndAfterAll {

  val zkServer: EmbeddedZookeeperServer = new EmbeddedZookeeperServer()
  val kafkaCluster: EmbeddedKafkaCluster = new EmbeddedKafkaCluster(zkServer.getConnection)
  val vkitmServer: EmbeddedVKitM = new EmbeddedVKitM(zkServer.getConnection, kafkaCluster.getBrokerList)

  val kafkaProducer = TestUtils.buildProducer(kafkaCluster.getBrokerList)
  val kafkaConsumer = TestUtils.buildConsumer(kafkaCluster.getBrokerList)

  val vkitmProducer = TestUtils.buildProducer(vkitmServer.getBrokerList)
  val vkitmConsumer = TestUtils.buildConsumer(vkitmServer.getBrokerList)

  "A VKitM producer" when {
    "producing" should {
      val key = "test-key".getBytes
      val value = "test-value".getBytes

      "create a topic and publish the message if the topic doesn't exist" in {
        val topic = "topic1"
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
        kafkaCluster.existTopic(topic) should be(false)
        val metadata = vkitmProducer.send(record).get(1000, TimeUnit.MILLISECONDS)
        metadata.offset() should be(0)
        kafkaCluster.existTopic(topic) should be(true)
      }
      "increment the offset" in {
        val numMessages = 10
        val topic = "topic2"
        kafkaCluster.existTopic(topic) should be(false)
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
        for (i <- 0 to numMessages) {
          val metadata = vkitmProducer.send(record).get(1000, TimeUnit.MILLISECONDS)
          metadata.offset() should be(i)
        }
        kafkaCluster.existTopic(topic) should be(true)
      }
      "publish the same messages expected by the VKitM consumer" in {
        val numMessages = 10
        val topic = "topic3"
        kafkaCluster.createTopic(topic)
        kafkaCluster.existTopic(topic) should be(true)
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
        for (i <- 0 to numMessages) {
          val metadata = vkitmProducer.send(record).get(1000, TimeUnit.MILLISECONDS)
          metadata.offset() should be(i)
        }

        vkitmConsumer.subscribe(Seq(topic).asJava)
        val records: ConsumerRecords[Array[Byte], Array[Byte]] = vkitmConsumer.poll(2000)
        //TODO
        /*
        records.count() should be(numMessages)
        records.asScala.foreach { record =>
          record.topic() should be eq topic
        }
        */
      }
    }
  }

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
