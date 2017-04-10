package com.github.mmolimar.vkitm.common.cache

import java.util.{Properties, UUID}

import com.google.common.util.concurrent.UncheckedExecutionException
import kafka.server.KafkaConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.clients.{ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.Selectable
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.utils.Time
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class CacheTest extends WordSpec with MockFactory {

  "A client cache" when {
    "empty" should {
      val cache = Cache.forClients()
      "have size 0" in {
        cache.size should be(0)
      }
      "get null when retrieving a record" in {
        val ncr = NetworkClientRequest("test", null, null, null)
        cache.get(ncr) should be(null)
      }
    }
    "populating" should {
      val maxSize = 10
      val cache = Cache.forClients(maxSize = maxSize)
      val config = new KafkaConfig(Map(KafkaConfig.ZkConnectProp -> "zkhost:2181").asJava, false)
      val ncr = NetworkClientRequest(UUID.randomUUID().toString, mock[ManualMetadataUpdater], config, new Metrics())
      val networkClient = new NetworkClient(stub[Selectable], mock[ManualMetadataUpdater], "test-client", 0, 0, 0, 0, 0, mock[Time])
      cache.put(ncr, networkClient)

      "produce an UncheckedExecutionException when retrieving a bad key record" in {
        val ncrTest = NetworkClientRequest(UUID.randomUUID().toString, null, null, null)
        a[UncheckedExecutionException] should be thrownBy cache.getAndMaybePut(ncrTest)
      }

      "produce an IllegalArgumentException when putting a bad record" in {
        val ncrTest = NetworkClientRequest("test", null, null, null)
        a[IllegalArgumentException] should be thrownBy cache.put(ncrTest, mock[NetworkClient])
      }

      "get an existing record in cache" in {
        cache.contains(ncr) should be(true)
        cache.get(ncr) should not be (null)
      }

      "not put another record if already exists" in {
        val previous = cache.size
        cache.putIfNotExists(ncr, networkClient)
        cache.size should be(previous)
      }

      "put another record if doesn't exist" in {
        val previous = cache.size
        val ncrTest = NetworkClientRequest(UUID.randomUUID().toString, mock[ManualMetadataUpdater], config, new Metrics())
        cache.putIfNotExists(ncrTest, networkClient)
        cache.size should be(previous + 1)
      }

      "get and maybe put another record if doesn't exist" in {
        val previous = cache.size
        val ncrTest = NetworkClientRequest(UUID.randomUUID().toString, mock[ManualMetadataUpdater], config, new Metrics())
        cache.get(ncrTest) should be(null)
        cache.getAndMaybePut(ncrTest) should not be (null)
        cache.size should be(previous + 1)
      }

      "remove a key if record exists" in {
        val previous = cache.size
        val ncrTest = NetworkClientRequest(UUID.randomUUID().toString, mock[ManualMetadataUpdater], config, new Metrics())
        cache.remove(ncrTest)
        cache.size should be(previous)

        cache.remove(ncr)
        cache.size should be(previous - 1)
      }

      "not exceed max size when putting multiple records" in {
        for (i <- 1 to maxSize * 2) {
          val ncrTest = NetworkClientRequest(UUID.randomUUID().toString, mock[ManualMetadataUpdater], config, new Metrics())
          cache.put(ncrTest, networkClient)
          cache.size should not be >(maxSize)
        }
        cache.size should be(maxSize)
      }
    }
  }

  "A producer cache" when {
    "empty" should {
      val cache = Cache.forProducers()
      "have size 0" in {
        cache.size should be(0)
      }
      "get null when retrieving a record" in {
        val cpr = ClientProducerRequest("test", "broker:9092", 0)
        cache.get(cpr) should be(null)
      }
    }
    "populating" should {
      val maxSize = 10
      val cache = Cache.forProducers(maxSize = maxSize)
      val cpr = ClientProducerRequest(UUID.randomUUID().toString, "localhost:9092", 0)
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      val kafkaProducer = new KafkaProducer(props, new ByteArraySerializer, new ByteArraySerializer)
      cache.put(cpr, kafkaProducer)

      "produce an UncheckedExecutionException when host is unresolvable" in {
        val cprTest = ClientProducerRequest(UUID.randomUUID().toString, "test-broker:9092", 0)
        a[UncheckedExecutionException] should be thrownBy cache.getAndMaybePut(cprTest)
      }

      "get an existing record in cache" in {
        cache.contains(cpr) should be(true)
        cache.get(cpr) should not be (null)
      }

      "not put another record if already exists" in {
        val previous = cache.size
        cache.putIfNotExists(cpr, kafkaProducer)
        cache.size should be(previous)
      }

      "put another record if doesn't exist" in {
        val previous = cache.size
        val cprTest = ClientProducerRequest(UUID.randomUUID().toString, "localhost:9092", 0)
        cache.putIfNotExists(cprTest, kafkaProducer)
        cache.size should be(previous + 1)
      }

      "get and maybe put another record if doesn't exist" in {
        val previous = cache.size
        val cprTest = ClientProducerRequest(UUID.randomUUID().toString, "localhost:9092", 0)
        cache.get(cprTest) should be(null)
        cache.getAndMaybePut(cprTest) should not be (null)
        cache.size should be(previous + 1)
      }

      "remove a key if record exists" in {
        val previous = cache.size
        val cprTest = ClientProducerRequest(UUID.randomUUID().toString, "localhost:9092", 0)
        cache.remove(cprTest)
        cache.size should be(previous)

        cache.remove(cpr)
        cache.size should be(previous - 1)
      }

      "not exceed max size when putting multiple records" in {
        for (i <- 1 to maxSize * 2) {
          val cprTest = ClientProducerRequest(UUID.randomUUID().toString, "localhost:9092", 0)
          cache.put(cprTest, kafkaProducer)
          cache.size should not be >(maxSize)
        }
        cache.size should be(maxSize)
      }
    }
  }


}