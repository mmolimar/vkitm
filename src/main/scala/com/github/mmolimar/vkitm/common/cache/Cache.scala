package com.github.mmolimar.vkitm.common.cache

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalListener, RemovalNotification}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.clients.{ApiVersions, NetworkClient}
import org.apache.kafka.common.network._
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._

object Cache {
  protected val DEFAULT_INITIAL_CAPACITY = 10
  private val DEFAULT_EXPIRATION_TIME = 60000
  private val DEFAULT_MAX_SIZE = 50

  val NETWORK_CLIENT_METRICS_PREFIX = "vkitm-network-client-"

  def forProducers(initialCapacity: Int = Cache.DEFAULT_INITIAL_CAPACITY,
                   expirationTime: Int = Cache.DEFAULT_EXPIRATION_TIME,
                   maxSize: Int = Cache.DEFAULT_MAX_SIZE) = {
    new Cache(
      initialCapacity,
      expirationTime,
      maxSize,
      new ProducerRemovalListener[ClientProducerRequest, KafkaProducer[Array[Byte], Array[Byte]]],
      loader = (cpr: ClientProducerRequest) => {
        val props = new Properties
        props.putAll(cpr.props)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, cpr.clientId)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cpr.brokerList)
        props.put(ProducerConfig.ACKS_CONFIG, cpr.acks.toString)
        new KafkaProducer(props, new ByteArraySerializer, new ByteArraySerializer)
      })
  }

  def forClients(initialCapacity: Int = Cache.DEFAULT_INITIAL_CAPACITY,
                 expirationTime: Int = Cache.DEFAULT_EXPIRATION_TIME,
                 maxSize: Int = Cache.DEFAULT_MAX_SIZE) = {
    new Cache(
      initialCapacity,
      expirationTime,
      maxSize,
      new ClientRemovalListener[NetworkClientRequest, NetworkClient],
      loader = (ncr: NetworkClientRequest) => {
        val time = Time.SYSTEM

        val channelBuilder = ChannelBuilders.clientChannelBuilder(
          ncr.config.interBrokerSecurityProtocol,
          JaasContext.Type.CLIENT,
          ncr.config,
          ncr.config.advertisedListeners.head.listenerName,
          ncr.config.saslMechanismInterBrokerProtocol,
          ncr.config.saslInterBrokerHandshakeRequestEnable)

        val selector = new Selector(
          ncr.config.connectionsMaxIdleMs,
          ncr.metrics,
          time,
          NETWORK_CLIENT_METRICS_PREFIX + ncr.clientId,
          channelBuilder
        )

        new NetworkClient(
          selector,
          ncr.metadataUpdater,
          ncr.config.brokerId.toString,
          100,
          0,
          0,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          ncr.config.requestTimeoutMs,
          time,
          false,
          new ApiVersions)
      })
  }
}

private[cache] class Cache[K <: CacheEntry, V <: AnyRef](initialCapacity: Int,
                                                         expirationTime: Int,
                                                         maxSize: Int,
                                                         listener: RemovalListener[K, V],
                                                         loader: K => V) {

  private implicit def toCacheLoader[K, V](f: K => V): CacheLoader[K, V] =
    new CacheLoader[K, V] {
      def load(key: K) = f(key)
    }

  private val cache = CacheBuilder.newBuilder()
    .initialCapacity(initialCapacity)
    .expireAfterAccess(expirationTime, TimeUnit.MILLISECONDS)
    .maximumSize(maxSize)
    .asInstanceOf[CacheBuilder[K, V]]
    .removalListener(listener)
    .build[K, V](loader)

  def put(key: K, value: V) = cache.put(key, value)

  def putIfNotExists(key: K, value: V) = if (!contains(key)) cache.put(key, value)

  def get(key: K) = cache.getIfPresent(key)

  def getAndMaybePut(key: K) = cache.get(key)

  def contains(key: K) = cache.getIfPresent(key) != null

  def remove(key: K) = cache.invalidate(key)

  def size = cache.size

}

private[cache] class ProducerRemovalListener[K <: ClientProducerRequest, V <: KafkaProducer[Array[Byte], Array[Byte]]]
  extends RemovalListener[K, V] {

  override def onRemoval(notification: RemovalNotification[K, V]): Unit = {
    notification.getValue.close()
  }
}

private[cache] class ClientRemovalListener[K <: NetworkClientRequest, V <: NetworkClient] extends RemovalListener[K, V] {

  override def onRemoval(notification: RemovalNotification[K, V]): Unit = {
    notification.getValue.close()

    val metrics = notification.getKey.metrics
    metrics.metrics.asScala
      .filter(_._1.name.startsWith(Cache.NETWORK_CLIENT_METRICS_PREFIX + notification.getKey.clientId))
      .foreach { m =>
        metrics.removeMetric(m._1)
      }
  }
}

