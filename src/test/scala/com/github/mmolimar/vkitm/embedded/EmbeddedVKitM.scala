package com.github.mmolimar.vkitm.embedded

import java.util.Properties

import com.github.mmolimar.vkitm.server.{VKitMConfig, VKitMServer}
import com.github.mmolimar.vkitm.utils.TestUtils
import kafka.server.KafkaConfig
import kafka.utils.Logging
import org.apache.kafka.clients.producer.ProducerConfig

class EmbeddedVKitM(zkConnection: String,
                    brokerList: String,
                    port: Int = TestUtils.getAvailablePort) extends Logging {

  private var vkitmServer: VKitMServer = null

  def startup() {
    info("Starting up VKitM server")

    val serverProps = new Properties
    serverProps.setProperty(KafkaConfig.ZkConnectProp, zkConnection)
    serverProps.setProperty(KafkaConfig.HostNameProp, "localhost")
    serverProps.setProperty(KafkaConfig.PortProp, port.toString)
    serverProps.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:" + port)

    val producerProps = new Properties
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)

    vkitmServer = new VKitMServer(VKitMConfig.fromProps(serverProps, producerProps))
    vkitmServer.startup()

    info("Started embedded VKitM server")
  }

  def shutdown() {
    vkitmServer.shutdown()
  }

  def getPort: Int = port

  def getBrokerList: String = "localhost:" + getPort

  def getServer: VKitMServer = vkitmServer

  override def toString: String = {
    val sb: StringBuilder = StringBuilder.newBuilder
    sb.append("VKitM{")
    sb.append("config='").append(vkitmServer.config).append('\'')
    sb.append('}')

    sb.toString
  }

}
