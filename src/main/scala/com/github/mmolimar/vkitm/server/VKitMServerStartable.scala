package com.github.mmolimar.vkitm.server

import com.typesafe.config.Config
import kafka.utils.Logging

object VKitMServerStartable {
  def fromProps(serverProps: Config, producerProps: Config) = {
    new VKitMServerStartable(VKitMConfig.fromProps(serverProps, producerProps))
  }
}

class VKitMServerStartable(val config: VKitMConfig) extends Logging {
  private val server = new VKitMServer(config)

  def startup() {
    try {
      server.startup()
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during VKitMServerStable startup. Prepare to shutdown", e)
        System.exit(1)
    }
  }

  def shutdown() {
    try {
      server.shutdown()
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during VKitMServerStable shutdown. Prepare to halt", e)
        Runtime.getRuntime.halt(1)
    }
  }

  def awaitShutdown() =
    server.awaitShutdown

}


