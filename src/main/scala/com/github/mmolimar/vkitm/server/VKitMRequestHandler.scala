package com.github.mmolimar.vkitm.server

import kafka.network._
import kafka.utils._
import org.apache.kafka.common.utils.Utils

class VKitMRequestHandler(id: Int,
                          vkId: Int,
                          val totalHandlerThreads: Int,
                          val requestChannel: RequestChannel,
                          apis: VKitMApis) extends Runnable with Logging {
  this.logIdent = "[VKitM Request Handler " + id + " on Virtual Broker " + vkId + "], "

  def run() {
    while (true) {
      try {
        var req: RequestChannel.Request = null
        do {
          req = requestChannel.receiveRequest(300)
        } while (req == null)

        if (req eq RequestChannel.AllDone) {
          debug("VKitM request handler %d on virtual broker %s received shutdown command".format(
            id, vkId))
          return
        }
        req.requestDequeueTimeMs = SystemTime.milliseconds

        debug("VKitM request handler %d on virtual broker %s handling request %s".format(id, vkId, req.requestId))
        apis.handle(req)
      } catch {
        case e: Throwable => error("Exception when handling request", e)
      }
    }
  }

  def shutdown(): Unit = requestChannel.sendRequest(RequestChannel.AllDone)
}

class VKitMRequestHandlerPool(val vkId: Int,
                              val requestChannel: RequestChannel,
                              val apis: VKitMApis,
                              numThreads: Int) extends Logging {

  this.logIdent = "[VKitM Request Handler on Virtual Broker " + vkId + "], "
  val threads = new Array[Thread](numThreads)
  val runnables = new Array[VKitMRequestHandler](numThreads)
  for (i <- 0 until numThreads) {
    runnables(i) = new VKitMRequestHandler(i, vkId, numThreads, requestChannel, apis)
    threads(i) = Utils.daemonThread("vkitm-request-handler-" + i, runnables(i))
    threads(i).start()
  }

  def shutdown() {
    info("shutting down")
    for (handler <- runnables)
      handler.shutdown
    for (thread <- threads)
      thread.join
    info("shutdown completely")
  }
}
