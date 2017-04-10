package integration.com.github.mmolimar.vkitm.embedded

import java.io.{File, IOException}
import java.net.InetSocketAddress

import integration.com.github.mmolimar.vkitm.utils.TestUtils
import kafka.utils.{CoreUtils, Logging}
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}

class EmbeddedZookeeperServer(private val port: Int = TestUtils.getAvailablePort,
                              private val tickTime: Int = 500) extends Logging {

  private val snapshotDir: File = TestUtils.constructTempDir("embeeded-zk/snapshot")
  private val logDir: File = TestUtils.constructTempDir("embeeded-zk/log")
  private val zookeeper: ZooKeeperServer = new ZooKeeperServer(snapshotDir, logDir, tickTime)
  private val factory: ServerCnxnFactory = ServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), 0)

  @throws[IOException]
  def startup() {
    info("Starting up embedded Zookeeper")

    factory.startup(zookeeper)

    info("Started embedded Zookeeper: " + getConnection)
  }

  def shutdown() {
    info("Shutting down embedded Zookeeper")

    CoreUtils.swallow(zookeeper.shutdown())
    CoreUtils.swallow(factory.shutdown())

    TestUtils.deleteFile(snapshotDir)
    TestUtils.deleteFile(logDir)
    TestUtils.deleteFile(logDir.getParentFile)

    info("Shutted down embedded Zookeeper")
  }

  def getPort: Int = port

  def getConnection: String = "localhost:" + getPort

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder("Zookeeper{")
    sb.append("connection=").append(getConnection)
    sb.append('}')

    sb.toString
  }
}
