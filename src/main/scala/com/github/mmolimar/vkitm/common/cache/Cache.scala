package com.github.mmolimar.vkitm.common.cache

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.github.mmolimar.vkitm.common.ClientProducerRequest
import com.google.common.cache.{Cache, CacheBuilder, CacheLoader}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.ByteArraySerializer

trait CacheEntry {
}

object Cache {
  private val DEFAULT_INITIAL_CAPACITY = 10
  private val DEFAULT_EXPIRATION_TIME = 60000
  private val DEFAULT_MAX_SIZE = 50

  def forProducers(initialCapacity: Int = Cache.DEFAULT_INITIAL_CAPACITY,
                   expirationTime: Int = Cache.DEFAULT_EXPIRATION_TIME,
                   maxSize: Int = Cache.DEFAULT_MAX_SIZE) = {
    buildCache(
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

  private def buildCache[K <: CacheEntry, V <: AnyRef](initialCapacity: Int,
                                                       expirationTime: Int,
                                                       maxSize: Int,
                                                       loader: K => V): Cache[K, V] = {

    implicit def toCacheLoader[K, V](f: K => V): CacheLoader[K, V] =
      new CacheLoader[K, V] {
        def load(key: K) = f(key)
      }

    CacheBuilder.newBuilder()
      .initialCapacity(initialCapacity)
      .expireAfterAccess(expirationTime, TimeUnit.MILLISECONDS)
      .maximumSize(maxSize)
      .asInstanceOf[CacheBuilder[K, V]]
      .build[K, V](loader)
  }

}


