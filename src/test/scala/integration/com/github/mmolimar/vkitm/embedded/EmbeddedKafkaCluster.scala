package integration.com.github.mmolimar.vkitm.embedded

import java.io.File
import java.util.Properties

import integration.com.github.mmolimar.vkitm.utils.TestUtils
import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{CoreUtils, Logging, ZkUtils}

class EmbeddedKafkaCluster(zkConnection: String,
                           ports: Seq[Int] = Seq(TestUtils.getAvailablePort),
                           baseProps: Properties = new Properties) extends Logging {

  private val actualPorts: Seq[Int] = ports.map { port =>
    resolvePort(port)
  }
  private var brokers: Seq[KafkaServer] = Seq.empty
  private var logDirs: Seq[File] = Seq.empty

  def startup() {
    info("Starting up embedded Kafka brokers")

    for ((port, i) <- actualPorts.zipWithIndex) {
      val logDir: File = TestUtils.constructTempDir("kafka-local")

      val properties: Properties = new Properties(baseProps)
      properties.setProperty(KafkaConfig.ZkConnectProp, zkConnection)
      properties.setProperty(KafkaConfig.ZkSyncTimeMsProp, i.toString)
      properties.setProperty(KafkaConfig.BrokerIdProp, (i + 1).toString)
      properties.setProperty(KafkaConfig.HostNameProp, "localhost")
      properties.setProperty(KafkaConfig.PortProp, port.toString)
      properties.setProperty(KafkaConfig.LogDirProp, logDir.getAbsolutePath)
      properties.setProperty(KafkaConfig.NumPartitionsProp, 1.toString)
      properties.setProperty(KafkaConfig.AutoCreateTopicsEnableProp, true.toString)
      properties.setProperty(KafkaConfig.LogFlushIntervalMessagesProp, 1.toString)

      info(s"Local directory for broker ID ${i + 1} is ${logDir.getAbsolutePath}")

      brokers :+= startBroker(properties)
      logDirs :+= logDir
    }

    info(s"Started embedded Kafka brokers: $getBrokerList")
  }

  def shutdown() {
    brokers.foreach(broker => CoreUtils.swallow(broker.shutdown()))
    logDirs.foreach(logDir => CoreUtils.swallow(TestUtils.deleteFile(logDir)))
  }

  def getPorts: Seq[Int] = actualPorts

  def getBrokerList: String = actualPorts.map("localhost:" + _).mkString(",")

  def createTopic(topic: String, partitionCount: Int) {
    info(s"Creating topic $topic with partitions $partitionCount")
    AdminUtils.createTopic(getZkUtils, topic, partitionCount, 1, new Properties)
  }

  def createTopics(topics: Seq[String]) {
    info(s"Creating topics $topics")
    for (topic <- topics) {
      AdminUtils.createTopic(getZkUtils, topic, 2, 1, new Properties)
    }
  }

  def deleteTopic(topic: String) {
    info(s"Deleting topics $topic")
    AdminUtils.deleteTopic(getZkUtils, topic)
  }

  def deleteTopics(topics: Seq[String]) {
    topics.foreach(deleteTopic(_))
  }

  def existTopic(topic: String): Boolean = AdminUtils.topicExists(getZkUtils, topic)

  def createTopic(topic: String) = AdminUtils.createTopic(getZkUtils, topic, 1, 1)

  private def getZkUtils: ZkUtils = if (brokers.isEmpty) null else brokers.head.zkUtils

  private def resolvePort(port: Int) = if (port <= 0) TestUtils.getAvailablePort else port

  private def startBroker(props: Properties): KafkaServer = {
    val server = new KafkaServer(new KafkaConfig(props))
    server.startup()
    server
  }

  override def toString: String = {
    val sb: StringBuilder = StringBuilder.newBuilder
    sb.append("Kafka{")
    sb.append("brokerList='").append(getBrokerList).append('\'')
    sb.append('}')

    sb.toString
  }

}
