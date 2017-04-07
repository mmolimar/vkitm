package com.github.mmolimar.vkitm

import java.util.Properties

import com.github.mmolimar.vkitm.server.{VKitMServer, VKitMServerStartable}
import joptsimple.OptionParser
import kafka.utils.{CommandLineUtils, Logging}
import org.apache.kafka.common.utils.Utils

object VKitM extends Logging {

  def getPropsFromArgs(args: Array[String], index: Int): Properties = {
    val optionParser = new OptionParser
    if (args.length == 0) {
      CommandLineUtils.printUsageAndDie(optionParser, "USAGE: java [options] %s server.properties producer.properties".format(classOf[VKitMServer].getSimpleName()))
    }

    Utils.loadProps(args(index))
  }

  def main(args: Array[String]): Unit = {
    try {
      val serverProps = getPropsFromArgs(args, 0)
      val producerProps = getPropsFromArgs(args, 1)
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
  }
}
