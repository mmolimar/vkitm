package com.github.mmolimar.kafka.vkitm.utils

import java.util.concurrent.{CancellationException, TimeUnit, Future => JFuture}

import org.jboss.netty.util.{HashedWheelTimer, Timeout, TimerTask}

import scala.concurrent.{Future, Promise}
import scala.util.Try

object Helpers {

  private val pollIntervalMs = 50L
  private val timer = new HashedWheelTimer(pollIntervalMs, TimeUnit.MILLISECONDS)

  implicit class JFutureHelpers[T](jf: JFuture[T]) {
    def asScala: Future[T] = {
      val promise = Promise[T]()

      def checkCompletion(): Unit = {
        if (jf.isCancelled) {
          promise.failure(new CancellationException())
        } else if (jf.isDone) {
          promise.complete(Try(jf.get))
        } else {
          scheduleTimeout()
        }
        ()
      }

      def scheduleTimeout(): Unit = {
        timer.newTimeout(new TimerTask {
          override def run(timeout: Timeout): Unit = checkCompletion()
        }, pollIntervalMs, TimeUnit.MILLISECONDS)
        ()
      }

      checkCompletion()
      promise.future
    }
  }

}
