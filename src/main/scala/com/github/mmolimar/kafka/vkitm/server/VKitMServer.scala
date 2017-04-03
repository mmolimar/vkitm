package com.github.mmolimar.kafka.vkitm.server

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import kafka.cluster.Broker
import kafka.common.KafkaException
import kafka.network.SocketServer
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, _}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.AppInfoParser

object VKitMServer {

  val DEFAULT_VKITM_BROKER_ID = 999999

}

class VKitMServer(val config: VKitMConfig, time: Time = SystemTime, threadNamePrefix: Option[String] = None) extends Logging {
  private val startupComplete = new AtomicBoolean(false)
  private val isShuttingDown = new AtomicBoolean(false)
  private val isStartingUp = new AtomicBoolean(false)

  private var shutdownLatch = new CountDownLatch(1)

  private val jmxPrefix: String = "vkitm.server"
  private val reporters: java.util.List[MetricsReporter] = config.serverConfig.metricReporterClasses
  reporters.add(new JmxReporter(jmxPrefix))

  private implicit val vkitmMetricsTime: org.apache.kafka.common.utils.Time = new org.apache.kafka.common.utils.SystemTime()
  var metrics: Metrics = null

  private val metricConfig: MetricConfig = new MetricConfig()
    .samples(config.serverConfig.metricNumSamples)
    .timeWindow(config.serverConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS)

  var apis: VKitMApis = null
  var socketServer: SocketServer = null
  var metadataCache: FakedMetadataCache = null
  var requestHandlerPool: VKitMRequestHandlerPool = null
  var fakedMetadataManager: MetadataManager = null

  val vkitmScheduler = new KafkaScheduler(config.serverConfig.backgroundThreads, "vkitm-scheduler-")

  var zkUtils: ZkUtils = null

  private var _clusterId: String = null

  def clusterId: String = _clusterId

  def startup() {
    try {
      info("starting")

      if (isShuttingDown.get)
        throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")

      if (startupComplete.get) return

      val canStartup = isStartingUp.compareAndSet(false, true)
      if (!canStartup) return

      metrics = new Metrics(metricConfig, reporters, vkitmMetricsTime, true)

      vkitmScheduler.startup()

      zkUtils = initZk()

      _clusterId = zkUtils.getClusterId.getOrElse(throw new KafkaException("Failed to get cluster id from Zookeeper. This can only happen if /cluster/id is deleted from Zookeeper."))
      info(s"Cluster ID = $clusterId")

      config.serverConfig.brokerId = VKitMServer.DEFAULT_VKITM_BROKER_ID
      this.logIdent = "[VKitM Server], "

      metadataCache = new FakedMetadataCache(config.serverConfig.brokerId)

      socketServer = new SocketServer(config.serverConfig, metrics, vkitmMetricsTime)
      socketServer.startup()

      val virtualBroker = new Broker(VKitMServer.DEFAULT_VKITM_BROKER_ID, config.serverConfig.advertisedListeners, config.serverConfig.rack)
      fakedMetadataManager = new MetadataManager(config.serverConfig.brokerId, zkUtils, Seq(virtualBroker), metadataCache)
      fakedMetadataManager.startup()

      apis = new VKitMApis(socketServer.requestChannel, zkUtils, config, metadataCache, metrics, clusterId)

      requestHandlerPool = new VKitMRequestHandlerPool(config.serverConfig.brokerId,
        socketServer.requestChannel, apis, config.serverConfig.numIoThreads)


      shutdownLatch = new CountDownLatch(1)
      startupComplete.set(true)
      isStartingUp.set(false)
      AppInfoParser.registerAppInfo(jmxPrefix, config.serverConfig.brokerId.toString)
      info("started")
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during VKitMServer startup. Prepare to shutdown", e)
        isStartingUp.set(false)
        shutdown()
        throw e
    }
  }

  private def initZk(): ZkUtils = {
    info(s"Connecting to zookeeper on ${config.serverConfig.zkConnect}")

    val chrootIndex = config.serverConfig.zkConnect.indexOf("/")
    val chrootOption = {
      if (chrootIndex > 0) Some(config.serverConfig.zkConnect.substring(chrootIndex))
      else None
    }

    val secureAclsEnabled = config.serverConfig.zkEnableSecureAcls
    val isZkSecurityEnabled = JaasUtils.isZkSecurityEnabled()

    if (secureAclsEnabled && !isZkSecurityEnabled)
      throw new java.lang.SecurityException(s"${KafkaConfig.ZkEnableSecureAclsProp} is true, but the verification of the JAAS login file failed.")

    ZkUtils(config.serverConfig.zkConnect,
      config.serverConfig.zkSessionTimeoutMs,
      config.serverConfig.zkConnectionTimeoutMs,
      secureAclsEnabled)
  }

  def shutdown() {
    try {
      info("shutting down")

      if (isStartingUp.get)
        throw new IllegalStateException("VKitM server is still starting up, cannot shut down!")

      val canShutdown = isShuttingDown.compareAndSet(false, true)
      if (canShutdown && shutdownLatch.getCount > 0) {
        if (socketServer != null)
          CoreUtils.swallow(socketServer.shutdown())
        if (requestHandlerPool != null)
          CoreUtils.swallow(requestHandlerPool.shutdown())
        CoreUtils.swallow(vkitmScheduler.shutdown())
        if (apis != null)
          CoreUtils.swallow(apis.close())
        if (fakedMetadataManager != null)
          CoreUtils.swallow(fakedMetadataManager.shutdown())
        if (zkUtils != null)
          CoreUtils.swallow(zkUtils.close())
        if (metrics != null)
          CoreUtils.swallow(metrics.close())

        startupComplete.set(false)
        isShuttingDown.set(false)
        AppInfoParser.unregisterAppInfo(jmxPrefix, config.serverConfig.brokerId.toString)
        shutdownLatch.countDown()
        info("shut down completed")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during VKitM server shutdown.", e)
        isShuttingDown.set(false)
        throw e
    }
  }

  def awaitShutdown(): Unit = shutdownLatch.await()

  def boundPort(protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int = socketServer.boundPort(protocol)
}
