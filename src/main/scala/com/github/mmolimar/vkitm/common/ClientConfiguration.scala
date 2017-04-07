package com.github.mmolimar.vkitm.common

import java.util.Properties

import com.github.mmolimar.vkitm.common.cache.CacheEntry

case class ClientProducerRequest(clientId: String,
                                 brokerList: String,
                                 acks: Int,
                                 props: Option[Properties] = None) extends CacheEntry