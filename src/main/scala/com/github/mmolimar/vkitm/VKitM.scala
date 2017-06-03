package com.github.mmolimar.vkitm

import java.io.File
import java.util.Date

import com.github.mmolimar.vkitm.server.VKitMServerStartable
import com.typesafe.config.ConfigFactory
import kafka.utils.Logging

object VKitM extends App with Logging {

  if (args.length != 1) {
    Console.err.println("Usage: VKitM <application.conf>")
    System.exit(1)
  }
  if (!new File(args(0)).exists) {
    Console.err.println("<application.conf> does not exist")
    System.exit(1)
  }

  var vkitmServerStartable: VKitMServerStartable = null
  try {
    val config = ConfigFactory.load(ConfigFactory.parseFile(new File(args(0)))).resolve
    val serverProps = config.getObject("server").toConfig
    val producerProps = config.getObject("producer").toConfig
    val consumerProps = config.getObject("consumer").toConfig

    showBanner

    vkitmServerStartable = VKitMServerStartable.fromProps(serverProps, producerProps, consumerProps)

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

  //just for testing purposes
  private[vkitm] def getVkitmServerStartable = vkitmServerStartable

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
