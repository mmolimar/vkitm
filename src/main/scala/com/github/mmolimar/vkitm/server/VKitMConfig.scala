package com.github.mmolimar.vkitm.server

import java.util.Properties

import com.typesafe.config.Config
import kafka.server.KafkaConfig

object VKitMConfig {

  import com.github.mmolimar.vkitm.utils.Helpers.propsFromConfig

  def fromProps(serverProps: Config, producerProps: Config): VKitMConfig =
    new VKitMConfig(KafkaConfig.fromProps(serverProps), producerProps)

  def fromProps(serverProps: Properties, producerProps: Properties): VKitMConfig =
    new VKitMConfig(KafkaConfig.fromProps(serverProps), producerProps)
}

class VKitMConfig(val serverConfig: KafkaConfig, val producerConfig: Properties) {

}
