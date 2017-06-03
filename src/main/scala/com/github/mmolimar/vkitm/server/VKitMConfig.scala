package com.github.mmolimar.vkitm.server

import java.util.Properties

import com.typesafe.config.Config
import kafka.server.KafkaConfig

object VKitMConfig {

  import com.github.mmolimar.vkitm.utils.Helpers.propsFromConfig

  def fromProps(serverProps: Config, producerProps: Config, consumerProps: Config): VKitMConfig =
    new VKitMConfig(KafkaConfig.fromProps(serverProps), producerProps, consumerProps)

  def fromProps(serverProps: Properties, producerProps: Properties, consumerProps: Properties): VKitMConfig =
    new VKitMConfig(KafkaConfig.fromProps(serverProps), producerProps, consumerProps)
}

private[server] class VKitMConfig(val serverConfig: KafkaConfig, val producerProps: Properties, val consumerProps: Properties) {

}
