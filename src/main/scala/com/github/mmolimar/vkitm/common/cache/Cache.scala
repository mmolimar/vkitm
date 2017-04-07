package com.github.mmolimar.vkitm.common.cache

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.ByteArraySerializer


object Cache {
  private val DEFAULT_INITIAL_CAPACITY = 10
  private val DEFAULT_EXPIRATION_TIME = 60000
  private val DEFAULT_MAX_SIZE = 50

  def forProducers(initialCapacity: Int = Cache.DEFAULT_INITIAL_CAPACITY,
                   expirationTime: Int = Cache.DEFAULT_EXPIRATION_TIME,
                   maxSize: Int = Cache.DEFAULT_MAX_SIZE) = {
    new Cache(
      initialCapacity,
      expirationTime,
      maxSize,
      loader = (cpr: ClientProducerRequest) => {
        val props = cpr.props.getOrElse(new Properties)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, cpr.clientId)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cpr.brokerList)
        props.put(ProducerConfig.ACKS_CONFIG, cpr.acks.toString)
        new KafkaProducer(props, new ByteArraySerializer, new ByteArraySerializer)
      })
  }
}

private[cache] class Cache[K <: CacheEntry, V <: AnyRef](initialCapacity: Int,
                                                         expirationTime: Int,
                                                         maxSize: Int,
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
    .build[K, V](loader)

  def put(key: K, value: V) = cache.put(key, value)

  def putIfNotExists(key: K, value: V) = if (!contains(key)) cache.put(key, value)

  def get(key: K) = cache.getIfPresent(key)

  def getAndMaybePut(key: K) = cache.get(key)

  def contains(key: K) = cache.getIfPresent(key) == null

}


