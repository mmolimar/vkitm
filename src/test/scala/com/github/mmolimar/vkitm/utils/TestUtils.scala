package com.github.mmolimar.vkitm.utils

import java.io.{File, FileNotFoundException, IOException}
import java.net.{InetSocketAddress, ServerSocket}
import java.nio.channels.ServerSocketChannel
import java.util.{Properties, Random, UUID}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

object TestUtils {

  private val RANDOM: Random = new Random

  def constructTempDir(dirPrefix: String) = {
    val file: File = new File(System.getProperty("java.io.tmpdir"), dirPrefix + RANDOM.nextInt(10000000))
    if (!file.mkdirs) throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath)
    file.deleteOnExit()
    file
  }

  def getAvailablePort = {
    var socket: ServerSocket = null
    try {
      socket = new ServerSocket(0)
      socket.getLocalPort

    } catch {
      case e: IOException => throw new IllegalStateException("Cannot find available port: " + e.getMessage, e)
    }
    finally socket.close()
  }

  def waitTillAvailable(host: String, port: Int, maxWaitMs: Int) = {
    val defaultWait: Int = 100
    var currentWait: Int = 0
    try
        while (isPortAvailable(host, port) && currentWait < maxWaitMs) {
          Thread.sleep(defaultWait)
          currentWait += defaultWait
        }

    catch {
      case ie: InterruptedException => throw new RuntimeException(ie)
    }
  }

  def isPortAvailable(host: String, port: Int): Boolean = {
    var ss: ServerSocketChannel = null
    try {
      ss = ServerSocketChannel.open
      ss.socket.setReuseAddress(false)
      ss.socket.bind(new InetSocketAddress(host, port))
      true

    } catch {
      case ioe: IOException => false
    }
    finally if (ss != null) ss.close()
  }

  def buildProducer(brokerList: String, compression: String = "none"): KafkaProducer[Array[Byte], Array[Byte]] = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression)
    props.put(ProducerConfig.LINGER_MS_CONFIG, "0") //ensure writes are synchronous
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    new KafkaProducer(props, new ByteArraySerializer, new ByteArraySerializer)
  }

  def buildConsumer(brokerList: String, groupId: String = "test-group"): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val props = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-client-" + UUID.randomUUID)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0") //ensure we have no temporal batching

    new KafkaConsumer(props, new ByteArrayDeserializer, new ByteArrayDeserializer)
  }

  @throws[FileNotFoundException]
  def deleteFile(path: File): Boolean = {
    if (!path.exists) throw new FileNotFoundException(path.getAbsolutePath)
    var ret: Boolean = true
    if (path.isDirectory) for (f <- path.listFiles) {
      ret = ret && deleteFile(f)
    }
    ret && path.delete
  }

  def randomString(length: Int = 10): String = scala.util.Random.alphanumeric.take(length).mkString

}
