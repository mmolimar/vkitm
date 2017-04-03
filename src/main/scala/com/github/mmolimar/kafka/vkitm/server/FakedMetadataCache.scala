package com.github.mmolimar.kafka.vkitm.server

import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.api.PartitionStateInfo
import kafka.cluster.Broker
import kafka.common.{BrokerEndPointNotAvailableException, Topic, TopicAndPartition}
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.Node
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.MetadataResponse

import scala.collection.JavaConverters._
import scala.collection.{Seq, Set, mutable}

private[server] class FakedMetadataCache(vkId: Int) extends Logging {

  private val cache = mutable.Map[String, mutable.Map[Int, PartitionStateInfo]]()
  private val virtualAliveBrokers = mutable.Map[Int, Broker]()
  private val virtualAliveNodes = mutable.Map[Int, collection.Map[SecurityProtocol, Node]]()
  private val actualAliveBrokers = mutable.Map[Int, Broker]()
  private val actualAliveNodes = mutable.Map[Int, collection.Map[SecurityProtocol, Node]]()
  private val vkMetadataLock = new ReentrantReadWriteLock()
  private val vkBrokersLock = new ReentrantReadWriteLock()
  //the controller is always the same, the virtual broker
  private var controllerId: Option[Int] = Option(vkId)

  this.logIdent = s"[Virtual Kafka Metadata Cache on virtual broker $vkId] "


  def getAllTopics(): Set[String] = {
    inReadLock(vkMetadataLock) {
      cache.keySet.toSet
    }
  }

  def getNonExistingTopics(topics: Set[String]): Set[String] = {
    inReadLock(vkMetadataLock) {
      topics -- cache.keySet
    }
  }

  def getControllerId: Option[Int] = controllerId

  def getAliveBrokers: Seq[Broker] = {
    inReadLock(vkMetadataLock) {
      virtualAliveBrokers.values.toBuffer
    }
  }

  def update(updatedCache: mutable.Map[String, mutable.Map[Int, PartitionStateInfo]]) = {
    inWriteLock(vkMetadataLock) {
      cache.clear()
      updatedCache.foreach(t => cache.put(t._1, t._2))
    }
  }

  def removeTopics(topicsToBeDeleted: Set[String]) = {
    inWriteLock(vkMetadataLock) {
      topicsToBeDeleted.foreach(cache.remove(_))
    }
  }

  def setActualAliveBrokers(aliveBrokers: Seq[(Int, Broker)]) = {
    inWriteLock(vkBrokersLock) {
      actualAliveBrokers.clear()
      aliveBrokers.foreach { b =>
        actualAliveBrokers.put(b._1, b._2)
        actualAliveNodes.put(b._1, nodeByProtocol(b._2))
      }
    }
  }

  def setVirtualAliveBrokers(aliveBrokers: Seq[(Int, Broker)]) = {
    inWriteLock(vkBrokersLock) {
      virtualAliveBrokers.clear()
      virtualAliveNodes.clear()
      aliveBrokers.foreach { b =>
        virtualAliveBrokers.put(b._1, b._2)
        virtualAliveNodes.put(b._1, nodeByProtocol(b._2))
      }
    }
  }

  def contains(topic: String): Boolean = {
    inReadLock(vkMetadataLock) {
      cache.contains(topic)
    }
  }

  def getTopicMetadata(topics: Set[String], protocol: SecurityProtocol, errorUnavailableEndpoints: Boolean = false): Seq[MetadataResponse.TopicMetadata] = {
    inReadLock(vkMetadataLock) {
      topics.toSeq.flatMap { topic =>
        getPartitionMetadata(topic, protocol, errorUnavailableEndpoints).map { partitionMetadata =>
          new MetadataResponse.TopicMetadata(Errors.NONE, topic, Topic.isInternal(topic), partitionMetadata.toBuffer.asJava)
        }
      }
    }
  }

  private def getPartitionMetadata(topic: String, protocol: SecurityProtocol, errorUnavailableEndpoints: Boolean): Option[Iterable[MetadataResponse.PartitionMetadata]] = {
    cache.get(topic).map { partitions =>
      partitions.map { case (partitionId, partitionState) =>
        val topicPartition = TopicAndPartition(topic, partitionId)

        val leaderAndIsr = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr
        val maybeLeader = getAliveEndpoint(leaderAndIsr.leader, protocol)

        val replicas = partitionState.allReplicas
        val replicaInfo = getEndpoints(replicas, protocol, errorUnavailableEndpoints)

        maybeLeader match {
          case None =>
            debug(s"Error while fetching metadata for $topicPartition: leader not available")
            new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE, partitionId, Node.noNode(),
              replicaInfo.asJava, java.util.Collections.emptyList())

          case Some(leader) =>
            val isr = leaderAndIsr.isr
            val isrInfo = getEndpoints(isr, protocol, errorUnavailableEndpoints)

            if (replicaInfo.size < replicas.size) {
              debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                s"following brokers ${replicas.filterNot(replicaInfo.map(_.id).contains).mkString(",")}")

              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                replicaInfo.asJava, isrInfo.asJava)
            } else if (isrInfo.size < isr.size) {
              debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                s"following brokers ${isr.filterNot(isrInfo.map(_.id).contains).mkString(",")}")
              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                replicaInfo.asJava, isrInfo.asJava)
            } else {
              new MetadataResponse.PartitionMetadata(Errors.NONE, partitionId, leader, replicaInfo.asJava,
                isrInfo.asJava)
            }
        }
      }
    }
  }

  private def getEndpoints(brokers: Iterable[Int], protocol: SecurityProtocol, filterUnavailableEndpoints: Boolean): Seq[Node] = {
    val result = new mutable.ArrayBuffer[Node](math.min(virtualAliveBrokers.size, brokers.size))
    brokers.foreach { brokerId =>
      val endpoint = getAliveEndpoint(brokerId, protocol) match {
        case None => if (!filterUnavailableEndpoints) Some(new Node(brokerId, "", -1)) else None
        case Some(node) => Some(node)
      }
      endpoint.foreach(result +=)
    }
    result
  }

  private def getAliveEndpoint(brokerId: Int, protocol: SecurityProtocol): Option[Node] =
    virtualAliveNodes.get(brokerId).map { nodeMap =>
      nodeMap.getOrElse(protocol,
        throw new BrokerEndPointNotAvailableException(s"Broker `$brokerId` does not support security protocol `$protocol`"))
    }

  private def nodeByProtocol(broker: Broker): collection.Map[SecurityProtocol, Node] = {
    broker.endPoints.map { endPoint =>
      val brokerEndPoint = broker.getBrokerEndPoint(endPoint._1)
      (endPoint._1, new Node(brokerEndPoint.id, brokerEndPoint.host, brokerEndPoint.port))
    }
  }
}
