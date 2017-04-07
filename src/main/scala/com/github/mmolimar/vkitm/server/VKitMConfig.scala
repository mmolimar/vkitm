package com.github.mmolimar.vkitm.server

import java.util.Properties

import kafka.server.KafkaConfig

object VKitMConfig {
  def fromProps(serverProps: Properties, producerProps: Properties): VKitMConfig =
    new VKitMConfig(KafkaConfig.fromProps(serverProps), producerProps)
}

class VKitMConfig(val serverConfig: KafkaConfig, val producerConfig: Properties) {

}
