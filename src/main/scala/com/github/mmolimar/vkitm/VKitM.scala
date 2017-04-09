package com.github.mmolimar.vkitm

import java.util.Date

import com.github.mmolimar.vkitm.server.VKitMServerStartable
import kafka.utils.Logging

object VKitM extends App with Logging {

  import com.github.mmolimar.vkitm.utils.Helpers.config

  showBanner

  try {
    val serverProps = config.getObject("server").toConfig
    val producerProps = config.getObject("producer").toConfig
    val vkitmServerStartable = VKitMServerStartable.fromProps(serverProps, producerProps)

    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run() = {
        vkitmServerStartable.shutdown
      }
    })

    vkitmServerStartable.startup
    vkitmServerStartable.awaitShutdown
  }
  catch {
    case e: Throwable =>
      fatal(e)
      System.exit(1)
  }
  System.exit(0)

  private def showBanner = {
    info(
      s"""
         |=====================================================
         | _    __    __ __    _    __     __  ___
         || |  / /   / //_/   (_)  / /_   /  |/  /
         || | / /   / ,<     / /  / __/  / /|_/ /
         || |/ /   / /| |   / /  / /_   / /  / /
         ||___/   /_/ |_|  /_/  /___/  /_/  /_/
         |                                         Mario Molina
         |
         |> Start time    : ${new Date()}
         |> Number of CPUs: ${Runtime.getRuntime.availableProcessors}
         |> Total memory  : ${Runtime.getRuntime.totalMemory}
         |> Free memory   : ${Runtime.getRuntime.freeMemory}
         |=====================================================
    """.stripMargin)

  }
}
