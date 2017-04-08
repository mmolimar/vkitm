package com.github.mmolimar.vkitm.common.cache

import java.util.Properties

import kafka.server.KafkaConfig
import org.apache.kafka.clients.ManualMetadataUpdater
import org.apache.kafka.common.metrics.Metrics

private[cache] trait CacheEntry {
}

case class ClientProducerRequest(clientId: String,
                                 brokerList: String,
                                 acks: Short,
                                 props: Option[Properties] = None) extends CacheEntry

case class NetworkClientRequest(clientId: String,
                                metadataUpdater: ManualMetadataUpdater,
                                config: KafkaConfig,
                                metrics: Metrics) extends CacheEntry
