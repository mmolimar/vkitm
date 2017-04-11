package com.github.mmolimar.vkitm

import java.security.Permission

import com.github.mmolimar.vkitm.embedded.{EmbeddedKafkaCluster, EmbeddedZookeeperServer}
import com.github.mmolimar.vkitm.utils.TestUtils
import kafka.utils.Logging
import org.junit.runner.RunWith
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, WordSpec}

import scala.reflect.io.File

@RunWith(classOf[JUnitRunner])
class VKitMTest extends WordSpec with BeforeAndAfterAll with Logging {

  val args = Array("src/test/resources/application.conf")
  var securityManager = System.getSecurityManager

  "VKitM" when {
    "starting up" should {

      "fail if program args are invalid" in {
        a[SystemExit_1_Exception] should be thrownBy VKitM.main(Array.empty)
      }

      "fail if application config file does not exist" in {
        a[SystemExit_1_Exception] should be thrownBy VKitM.main(Array(TestUtils.randomString()))
      }

      "fail if application config file is empty" in {
        a[SystemExit_1_Exception] should be thrownBy VKitM.main(Array(File.makeTemp().toString))
      }

      "fail if ZK is not running" in {
        a[SystemExit_1_Exception] should be thrownBy VKitM.main(args)
      }

      "fail if ZK is running but Kafka is not" in {
        val zkServer = createZkServer()
        zkServer.startup
        TestUtils.waitTillAvailable("localhost", zkServer.getPort, 5000)

        try
          a[SystemExit_1_Exception] should be thrownBy VKitM.main(args)
        finally
          zkServer.shutdown
      }

      "start successfully if ZK and Kafka are up and running" in {
        val zkServer = createZkServer()
        val kafkaCluster = createKafkaServer(zkServer.getConnection)

        zkServer.startup
        TestUtils.waitTillAvailable("localhost", zkServer.getPort, 5000)

        kafkaCluster.startup
        kafkaCluster.getPorts.foreach { port =>
          TestUtils.waitTillAvailable("localhost", port, 5000)
        }

        try {
          new Thread() {
            override def run = {
              //wait till server is up
              for (i <- 1 to 10 if (VKitM.getVkitmServerStartable == null ||
                VKitM.getVkitmServerStartable.server == null ||
                !VKitM.getVkitmServerStartable.server.startupComplete.get)) {
                Thread.sleep(500)
              }
              VKitM.getVkitmServerStartable.shutdown
            }
          }.start
          //the applications finished successfully
          a[SystemExit_0_Exception] should be thrownBy VKitM.main(args)

        } finally {
          kafkaCluster.shutdown
          zkServer.shutdown
        }
      }
    }
  }

  private def createZkServer(): EmbeddedZookeeperServer = {
    new EmbeddedZookeeperServer(System.getProperty("TEST_ZK_PORT").toInt)
  }

  private def createKafkaServer(zkConnection: String): EmbeddedKafkaCluster = {
    new EmbeddedKafkaCluster(zkConnection, Seq(System.getProperty("TEST_KAFKA_PORT").toInt))
  }

  override def beforeAll {
    System.setProperty("TEST_ZK_PORT", TestUtils.getAvailablePort.toString)
    System.setProperty("TEST_KAFKA_PORT", TestUtils.getAvailablePort.toString)
    System.setProperty("TEST_VKITM_PORT", TestUtils.getAvailablePort.toString)

    securityManager = System.getSecurityManager
    System.setSecurityManager(new SecurityManager {
      override def checkExit(status: Int) = {
        info(s"Got exit code: $status")
        status match {
          case 0 => throw new SystemExit_0_Exception
          case 1 => throw new SystemExit_1_Exception
          case _ => throw new RuntimeException(s"Invalid status code: $status")
        }

      }

      override def checkPermission(perm: Permission) = {}

      override def checkPermission(perm: Permission, context: Any) = {}
    })
  }

  override def afterAll {
    System.setSecurityManager(securityManager)

    System.setProperty("TEST_VKITM_PORT", "")
    System.setProperty("TEST_ZK_PORT", "")
    System.setProperty("TEST_KAFKA_PORT", "")
  }

  private[vkitm] class SystemExit_0_Exception extends RuntimeException

  private[vkitm] class SystemExit_1_Exception extends RuntimeException

}
