package com.github.mmolimar.kafka.vkitm.server

import java.util.concurrent.atomic.AtomicBoolean

import kafka.api.{LeaderAndIsr, PartitionStateInfo}
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.utils.ZkUtils._
import kafka.utils.{Logging, ZkUtils}
import org.I0Itec.zkclient.IZkChildListener
import org.apache.kafka.common.requests.PartitionState

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import scala.collection.{JavaConversions, Seq, Set, mutable}


class MetadataManager(vkId: Int,
                      zkUtils: ZkUtils,
                      private val virtualBrokers: Seq[Broker],
                      private val metadataCache: FakedMetadataCache) extends Logging {

  private val hasStarted = new AtomicBoolean(false)
  private val topicChangeListener = new TopicChangeListener()
  private val deleteTopicListener = new DeleteTopicListener()
  private val brokerChangeListener = new BrokerChangeListener()

  this.logIdent = "[Metadata Manager on Virtual Broker " + vkId + "]: "


  def startup() {
    registerListeners()
    initialize()
    hasStarted.set(true)

    info("Started Metadata Manager.")
  }

  def shutdown() {
    deregisterListeners()
    hasStarted.set(false)
    info("Stopped Metadata Manager.")
  }

  private def initialize() {
    //set current existing topics
    val topics = zkUtils.getAllTopics()
    metadataCache.update(createTopicsByPartitionStateInfo(topics))

    //set current alive brokers
    val aliveBrokers = zkUtils.getAllBrokersInCluster().map { broker =>
      (broker.id, broker)
    }
    metadataCache.setActualAliveBrokers(aliveBrokers)

    //set virutal alive brokers
    metadataCache.setVirtualAliveBrokers(virtualBrokers.map(broker => (broker.id, broker)))
  }

  private def registerListeners() {
    registerTopicChangeListener()
    registerDeleteTopicListener()
    registerBrokerChangeListener()
  }

  private def deregisterListeners() {
    deregisterTopicChangeListener()
    deregisterDeleteTopicListener()
    deregisterBrokerChangeListener()
  }

  private def registerTopicChangeListener() = {
    zkUtils.zkClient.subscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  private def deregisterTopicChangeListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  private def registerDeleteTopicListener() = {
    zkUtils.zkClient.subscribeChildChanges(DeleteTopicsPath, deleteTopicListener)
  }

  private def deregisterDeleteTopicListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(DeleteTopicsPath, deleteTopicListener)
  }

  private def registerBrokerChangeListener() = {
    zkUtils.zkClient.subscribeChildChanges(BrokerIdsPath, brokerChangeListener)
  }

  private def deregisterBrokerChangeListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(BrokerIdsPath, brokerChangeListener)
  }


  private[server] class TopicChangeListener extends IZkChildListener with Logging {
    this.logIdent = "[TopicChangeListener]: "

    def handleChildChange(parentPath: String, children: java.util.List[String]) {
      if (!hasStarted.get) return

      val currentTopics = {
        import JavaConversions._
        debug("Topic change listener fired for path %s with children %s".format(parentPath, children.mkString(",")))
        (children: Buffer[String]).toSet
      }
      val newTopics = currentTopics -- metadataCache.getAllTopics
      val deletedTopics = metadataCache.getAllTopics -- currentTopics

      metadataCache.update(createTopicsByPartitionStateInfo(currentTopics.toSeq))

      info("New topics: [%s], deleted topics: [%s]".format(newTopics, deletedTopics))
    }
  }

  private[server] class DeleteTopicListener extends IZkChildListener with Logging {
    this.logIdent = "[DeleteTopicListener]: "

    def handleChildChange(parentPath: String, children: java.util.List[String]) {
      if (!hasStarted.get) return

      var topicsToBeDeleted = {
        import JavaConversions._
        (children: Buffer[String]).toSet
      }

      debug("Delete topics listener fired for topics %s to be deleted".format(topicsToBeDeleted.mkString(",")))
      val nonExistentTopics = topicsToBeDeleted -- metadataCache.getAllTopics
      topicsToBeDeleted --= nonExistentTopics

      if (topicsToBeDeleted.nonEmpty) {
        //cleaning topics from cache
        metadataCache.removeTopics(topicsToBeDeleted)
      }
    }
  }

  private[server] class BrokerChangeListener extends IZkChildListener with Logging {
    this.logIdent = "[BrokerChangeListener]: "

    def handleChildChange(parentPath: String, children: java.util.List[String]) {
      if (!hasStarted.get) return

      var currentBrokers = {
        import JavaConversions._
        (children: Buffer[String]).toSet
      }

      val aliveBrokers = zkUtils.getAllBrokersInCluster().map { broker =>
        (broker.id, broker)
      }

      metadataCache.setActualAliveBrokers(aliveBrokers)

      debug("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokers.mkString(",")))
    }
  }

  private def createTopicsByPartitionStateInfo(topics: Seq[String]): mutable.Map[String, mutable.Map[Int, PartitionStateInfo]] = {
    val topicAndPartitions = zkUtils.getPartitionsForTopics(topics).flatMap { pft =>
      pft._2.map(TopicAndPartition(pft._1, _))
    }.toSet

    val leaderAndIsrInfo: mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] =
      zkUtils.getPartitionLeaderAndIsrForTopics(zkUtils.zkClient, topicAndPartitions)

    leaderAndIsrInfo.map { info =>
      (info._1.topic, mutable.Map[Int, PartitionStateInfo](info._1.partition -> getPartitionStateInfo(info._2)))
    }

  }

  private def getPartitionStateInfo(leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch): PartitionStateInfo = {
    partitionStateToPartitionStateInfo(
      new PartitionState(leaderIsrAndControllerEpoch.controllerEpoch,
        vkId,
        leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch,
        List(vkId).map(Integer.valueOf).asJava,
        leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion,
        Set(vkId).map(Integer.valueOf).asJava))
  }

  private def partitionStateToPartitionStateInfo(partitionState: PartitionState): PartitionStateInfo = {
    val leaderAndIsr = LeaderAndIsr(partitionState.leader, partitionState.leaderEpoch, partitionState.isr.asScala.map(_.toInt).toList, partitionState.zkVersion)
    val leaderInfo = LeaderIsrAndControllerEpoch(leaderAndIsr, partitionState.controllerEpoch)
    PartitionStateInfo(leaderInfo, partitionState.replicas.asScala.map(_.toInt))
  }

}
