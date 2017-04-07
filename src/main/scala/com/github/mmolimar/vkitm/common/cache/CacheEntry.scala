package com.github.mmolimar.vkitm.common.cache

import java.util.Properties

private[cache] trait CacheEntry {
}

case class ClientProducerRequest(clientId: String,
                                 brokerList: String,
                                 acks: Short,
                                 props: Option[Properties] = None) extends CacheEntry
