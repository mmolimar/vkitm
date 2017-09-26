package com.github.mmolimar.vkitm.server

import java.io.IOException
import java.net.SocketTimeoutException
import java.nio.ByteBuffer
import java.util.concurrent.{Future => JFuture}

import com.github.mmolimar.vkitm.common.cache.{Cache, ClientProducerRequest, NetworkClientRequest}
import com.github.mmolimar.vkitm.utils.Helpers.JFutureHelpers
import kafka.api.ControlledShutdownRequest
import kafka.common._
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.{ClientQuotaManagerConfig, KafkaConfig}
import kafka.utils._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.clients.{ClientResponse, NetworkClientUtils}
import org.apache.kafka.common.errors.{ApiException, ClusterAuthorizationException, NetworkException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors, Protocol, SecurityProtocol}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails
import org.apache.kafka.common.requests.MetadataResponse.{PartitionMetadata, TopicMetadata}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Promise, Future => SFuture}
import scala.util.{Failure, Success}

class VKitMApis(val requestChannel: RequestChannel,
                val zkUtils: ZkUtils,
                val config: VKitMConfig,
                val metadataManager: MetadataManager,
                val metrics: Metrics,
                val quotas: QuotaManagers,
                val clusterId: String,
                time: Time) extends Logging {

  private val producerCache = Cache.forProducers()
  private val clientCache = Cache.forClients()
  private val consumerConfig = KafkaConfig.fromProps(config.consumerProps)
  private val metadataCache = metadataManager.metadataCache

  this.logIdent = "[VKitMApi-%d] ".format(config.serverConfig.brokerId)

  def handle(request: RequestChannel.Request) {
    try {
      trace("Handling request:%s from connection %s;securityProtocol:%s,principal:%s".
        format(request.requestDesc(true), request.connectionId, request.securityProtocol, request.session.principal))
      ApiKeys.forId(request.requestId) match {
        //by now, some ApiKeys are supported
        case ApiKeys.PRODUCE => handleProducerRequest(request)
        case ApiKeys.FETCH => handleFetchRequest(request)
        case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
        case ApiKeys.METADATA => handleTopicMetadataRequest(request)
        case ApiKeys.UPDATE_METADATA_KEY => handleUpdateMetadataRequest(request)
        case ApiKeys.FIND_COORDINATOR => handleFindCoordinatorRequest(request)
        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request)
        case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request)
        case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request)
        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case ApiKeys.CREATE_TOPICS => handleCreateTopicsRequest(request)
        case ApiKeys.DELETE_TOPICS => handleDeleteTopicsRequest(request)
        case requestId => throw new KafkaException("Unknown api code " + requestId)
      }
    } catch {
      case e: Throwable => handleError(request, e)
    } finally {
      request.apiLocalCompleteTimeNanos = time.nanoseconds
    }
  }

  def handleProducerRequest(request: RequestChannel.Request) {
    val produceRequest = request.body[ProduceRequest]
    val numBytesAppended = request.header.toStruct.sizeOf + request.bodyAndSize.size

    def sendRecord(topicPartition: TopicPartition, buffer: ByteBuffer): Seq[SFuture[(TopicPartition, PartitionResponse)]] = {

      def transform(futures: Seq[SFuture[RecordMetadata]]) = {

        implicit def makeResponse(p: AnyRef): (TopicPartition, PartitionResponse) = p match {
          case rm: RecordMetadata =>
            (topicPartition, new PartitionResponse(Errors.NONE, rm.offset, rm.timestamp))
          case t: Throwable => {
            val cause = if (t.getCause != null) t.getCause else t
            (topicPartition, new PartitionResponse(Errors.forException(cause)))
          }
        }

        for (f <- futures) yield {
          val p = Promise[(TopicPartition, PartitionResponse)]
          f.onComplete {
            //all is mapped as a success response, then make the custom partition response
            case Success(s) => p.success(s)
            case Failure(f) => p.success(f)
          }
          p.future
        }
      }

      val futures = MemoryRecords.readableRecords(buffer).batches.asScala.flatMap(_.asScala.map { rm =>
        val key = {
          rm.key match {
            case null => null
            case _ => rm.key.array.slice(rm.key.arrayOffset, rm.key.arrayOffset + rm.keySize)
          }
        }
        val value = {
          rm.value match {
            case null => null
            case _ => rm.value.array.slice(rm.value.arrayOffset, rm.value.arrayOffset + rm.valueSize)
          }
        }
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topicPartition.topic, key, value)
        val entry = ClientProducerRequest(request.header.clientId, getBrokerList, produceRequest.acks)(config.producerProps)
        producerCache.getAndMaybePut(entry).send(record).asScala

      }).toSeq

      transform(futures)
    }

    val result: Seq[SFuture[(TopicPartition, PartitionResponse)]] = produceRequest.partitionRecordsOrFail.asScala.map {
      case (topicPartition, memoryRecords) => sendRecord(topicPartition, memoryRecords.buffer)
    }.flatten.toSeq

    SFuture.sequence(result).onComplete { r =>
      val responsesByTopicPartition: Map[TopicPartition, PartitionResponse] = r.get.map { item =>
        item._1 -> item._2
      }.toMap

      var errorInResponse = false
      responsesByTopicPartition.foreach { case (topicPartition, status) =>
        if (status.error != Errors.NONE) {
          errorInResponse = true
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            status.error.exceptionName))
        }
      }

      def produceResponseCallback(bandwidthThrottleTimeMs: Int) {
        if (produceRequest.acks == 0) {
          val action =
            if (errorInResponse) {
              val exceptionsSummary = responsesByTopicPartition.map { case (topicPartition, status) =>
                topicPartition -> status.error.exceptionName
              }.mkString(", ")

              info(
                s"Closing connection due to error during produce request with correlation id ${request.header.correlationId} " +
                  s"from client id ${request.header.clientId} with ack=0\n" +
                  s"Topic and partition to exceptions: $exceptionsSummary"
              )
              RequestChannel.CloseConnectionAction
            } else RequestChannel.NoOpAction

          sendResponseExemptThrottle(new RequestChannel.Response(request, None, action))

        } else {
          sendResponseMaybeThrottle(request, requestThrottleMs =>
            new ProduceResponse(responsesByTopicPartition.asJava, requestThrottleMs))
        }
      }

      request.apiRemoteCompleteTimeNanos = time.nanoseconds

      quotas.produce.recordAndMaybeThrottle(
        request.session.sanitizedUser,
        request.header.clientId,
        numBytesAppended,
        produceResponseCallback)

      produceRequest.clearPartitionRecords
    }

  }

  def handleFetchRequest(request: RequestChannel.Request) {

    val fetchRequest = request.body[FetchRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      fetchRequest, ncr, request.connectionId, request.header.correlationId,
      clientResponse => clientResponse.responseBody.asInstanceOf[FetchResponse])

  }

  def handleUpdateMetadataRequest(request: RequestChannel.Request) {

    //always fine, the metadata is updated when starting the app and by the zkNode change listeners
    sendResponseMaybeThrottle(request, requestThrottleMs => new UpdateMetadataResponse(Errors.NONE))

  }

  def handleListOffsetRequest(request: RequestChannel.Request) {

    val offsetRequest = request.body[ListOffsetRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      offsetRequest, ncr, request.connectionId, request.header.correlationId,
      clientResponse => clientResponse.responseBody.asInstanceOf[ListOffsetResponse])

  }

  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = request.body[MetadataRequest]
    val requestVersion = request.header.apiVersion

    val topics =
      if (requestVersion == 0) {
        if (metadataRequest.topics == null || metadataRequest.topics.isEmpty)
          metadataCache.getAllTopics
        else
          metadataRequest.topics.asScala.toSet
      } else {
        if (metadataRequest.isAllTopics)
          metadataCache.getAllTopics
        else
          metadataRequest.topics.asScala.toSet
      }

    val errorUnavailableEndpoints = requestVersion == 0
    val topicMetadata = getTopicMetadata(request, topics, request.securityProtocol, errorUnavailableEndpoints)

    val nodes = metadataCache.getVirtualAliveNodes

    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(topicMetadata.mkString(","),
      nodes.mkString(","), request.header.correlationId, request.header.clientId))

    sendResponseMaybeThrottle(request, requestThrottleMs =>
      new MetadataResponse(
        requestThrottleMs,
        nodes.asJava,
        clusterId,
        metadataCache.getControllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID),
        topicMetadata.asJava
      ))

  }

  def handleFindCoordinatorRequest(request: RequestChannel.Request) {

    def fakeCoordinatorResponse(clientResponse: ClientResponse): FindCoordinatorResponse = {
      val actualResponse = clientResponse.responseBody.asInstanceOf[FindCoordinatorResponse]

      actualResponse.error match {
        case Errors.COORDINATOR_NOT_AVAILABLE =>
          new FindCoordinatorResponse(actualResponse.throttleTimeMs, actualResponse.error,
            Node.noNode)
        case _ =>
          new FindCoordinatorResponse(actualResponse.throttleTimeMs, actualResponse.error,
            metadataCache.getVirtualAliveNodes.head)
      }
    }

    val findCoordinatorRequest = request.body[FindCoordinatorRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      findCoordinatorRequest, ncr, request.connectionId, request.header.correlationId, fakeCoordinatorResponse)
  }

  def handleJoinGroupRequest(request: RequestChannel.Request) {

    val joinGroupRequest = request.body[JoinGroupRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      joinGroupRequest, ncr, request.connectionId, request.header.correlationId, clientResponse => {
        clientResponse.responseBody.asInstanceOf[JoinGroupResponse]
      })
  }

  def handleHeartbeatRequest(request: RequestChannel.Request) {

    val heartbeatRequest = request.body[HeartbeatRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      heartbeatRequest, ncr, request.connectionId, request.header.correlationId, clientResponse => {
        clientResponse.responseBody.asInstanceOf[JoinGroupResponse]
      })
  }

  def handleLeaveGroupRequest(request: RequestChannel.Request) {

    val leaveGroupRequest = request.body[LeaveGroupRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      leaveGroupRequest, ncr, request.connectionId, request.header.correlationId, clientResponse => {
        clientResponse.responseBody.asInstanceOf[LeaveGroupResponse]
      })
  }

  def handleSyncGroupRequest(request: RequestChannel.Request) {

    val syncGroupRequest = request.body[SyncGroupRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      syncGroupRequest, ncr, request.connectionId, request.header.correlationId, clientResponse => {
        clientResponse.responseBody.asInstanceOf[SyncGroupResponse]
      })
  }

  def handleApiVersionsRequest(request: RequestChannel.Request) {
    def sendResponseCallback(requestThrottleMs: Int) {
      val responseSend =
        if (Protocol.apiVersionSupported(ApiKeys.API_VERSIONS.id, request.header.apiVersion))
          ApiVersionsResponse.apiVersionsResponse(requestThrottleMs,
            config.serverConfig.interBrokerProtocolVersion.messageFormatVersion).toSend(request.connectionId, request.header)
        else ApiVersionsResponse.unsupportedVersionSend(request.connectionId, request.header)
      requestChannel.sendResponse(RequestChannel.Response(request, responseSend))
    }

    sendResponseMaybeThrottle(request, request.header.clientId, sendResponseCallback)

  }

  def handleCreateTopicsRequest(request: RequestChannel.Request) = {
    val createTopicsRequest = request.body[CreateTopicsRequest]
    val fakedTopics = createTopicsRequest.topics.asScala.mapValues(td => {
      new TopicDetails(td.numPartitions, td.replicationFactor, td.configs)
    })
    val fakedCreateTopicsRequest = new CreateTopicsRequest.Builder(fakedTopics.asJava,
      createTopicsRequest.timeout, createTopicsRequest.validateOnly).build(createTopicsRequest.version)

    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      fakedCreateTopicsRequest, ncr, request.connectionId, request.header.correlationId, clientResponse => {
        metadataManager.refreshTopics()
        clientResponse.responseBody.asInstanceOf[CreateTopicsResponse]
      })
  }

  def handleDeleteTopicsRequest(request: RequestChannel.Request) = {

    val deleteTopicRequest = request.body[DeleteTopicsRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      deleteTopicRequest, ncr, request.connectionId, request.header.correlationId, clientResponse => {
        metadataManager.refreshTopics()
        metadataCache.removeTopics(deleteTopicRequest.topics.asScala)
        clientResponse.responseBody.asInstanceOf[DeleteTopicsResponse]
      })

  }

  def close() {
    quotas.shutdown
    info("Shutdown complete.")
  }

  private def getTopicMetadata(request: RequestChannel.Request,
                               topics: Set[String],
                               securityProtocol: SecurityProtocol,
                               errorUnavailableEndpoints: Boolean): Seq[MetadataResponse.TopicMetadata] = {

    val topicResponses = metadataCache.getTopicMetadata(topics, securityProtocol, errorUnavailableEndpoints)
    if (topics.isEmpty || topicResponses.size == topics.size) {
      topicResponses
    } else {

      def fakePartitionMetadata(partitionMetadata: PartitionMetadata) = {
        val nodes = metadataCache.getVirtualAliveNodes
        val leader = nodes.head
        val replicas, isr = nodes.toList.asJava
        new PartitionMetadata(partitionMetadata.error, partitionMetadata.partition,
          leader, replicas, isr)
      }

      def errorResponse(t: Throwable) = {
        val nonExistentTopics = topics -- topicResponses.map(_.topic).toSet

        topicResponses ++ nonExistentTopics.map { topic =>
          new MetadataResponse.TopicMetadata(Errors.forException(t), topic, Topic.isInternal(topic),
            java.util.Collections.emptyList())
        }.toSeq
      }

      //must request update to the brokers
      val requestTime = Time.SYSTEM
      val node = metadataCache.getActualAliveNodes.head

      val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)
      val networkClient = clientCache.getAndMaybePut(ncr)

      val builder = new MetadataRequest.Builder(topics.toList.asJava, true)

      val clientRequest = networkClient.newClientRequest(node.idString, builder, requestTime.milliseconds, true)

      try {

        if (!NetworkClientUtils.awaitReady(networkClient, node, requestTime, config.serverConfig.requestTimeoutMs.longValue))
          throw new NetworkException(s"Failed to connect")

        val response = NetworkClientUtils.sendAndReceive(clientCache.getAndMaybePut(ncr), clientRequest, requestTime)

        val metadataResponse = response.responseBody.asInstanceOf[MetadataResponse]

        metadataResponse.topicMetadata.asScala.map { tm =>
          new TopicMetadata(tm.error, tm.topic, tm.isInternal,
            tm.partitionMetadata.asScala.map(fakePartitionMetadata(_)).asJava)
        }.toSeq

      } catch {
        case ioe: IOException => errorResponse(new NetworkException(ioe))
        case ae: ApiException => errorResponse(ae)
      }
    }
  }

  private def sendNetworkClientRequest(header: RequestHeader,
                                       request: RequestChannel.Request,
                                       jRequest: AbstractRequest,
                                       ncr: NetworkClientRequest,
                                       connectionId: String,
                                       correlationId: Int,
                                       buildResponse: ClientResponse => AbstractResponse,
                                       errorResponse: Option[Throwable => AbstractResponse] = None) {

    val requestTime = Time.SYSTEM
    val node = metadataCache.getActualAliveNodes.head

    val networkClient = clientCache.getAndMaybePut(ncr)

    val builder = new AbstractRequest.Builder[AbstractRequest](ApiKeys.forId(request.header.apiKey), request.header.apiVersion) {
      override def build(version: Short): AbstractRequest = jRequest
    }

    val clientRequest = networkClient.newClientRequest(node.idString, builder, requestTime.milliseconds, true)

    val response = {
      def resultException(t: Throwable) = {
        jRequest.getErrorResponse(header.apiVersion, t)
      }

      try {

        if (!NetworkClientUtils.awaitReady(networkClient, node, requestTime, config.serverConfig.requestTimeoutMs.longValue))
          throw new SocketTimeoutException(s"Failed to connect within ${config.serverConfig.requestTimeoutMs.longValue} ms")

        val response = NetworkClientUtils.sendAndReceive(clientCache.getAndMaybePut(ncr), clientRequest, requestTime)
        buildResponse(response)

      } catch {
        case ioe: IOException => errorResponse.getOrElse(resultException(_))(new NetworkException(ioe))
        case ae: ApiException => errorResponse.getOrElse(resultException(_))(ae)
      }
    }

    sendResponseMaybeThrottle(request, requestThrottleMs => response)
  }

  private def getBrokerList = {
    metadataCache.getActualAliveBrokers.map {
      _.endPoints.map {
        ep =>
          ep.host + ":" + ep.port
      }.mkString(",")
    }.mkString(",")
  }

  private def handleError(request: RequestChannel.Request, e: Throwable) {
    val mayThrottle = e.isInstanceOf[ClusterAuthorizationException] || !ApiKeys.forId(request.requestId).clusterAction
    if (request.requestObj != null) {
      def sendResponseCallback(requestThrottleMs: Int) {
        request.requestObj.handleError(e, requestChannel, request)
        error("Error when handling request %s".format(request.requestObj), e)
      }

      if (mayThrottle) {
        val clientId: String = request.requestObj match {
          case r: ControlledShutdownRequest => r.clientId.getOrElse("")
          case _ =>
            throw new IllegalStateException("Old style requests should only be used for ControlledShutdownRequest")
        }
        sendResponseMaybeThrottle(request, clientId, sendResponseCallback)
      } else
        sendResponseExemptThrottle(request, () => sendResponseCallback(0))
    } else {
      def createResponse(requestThrottleMs: Int): RequestChannel.Response = {
        val response = request.body[AbstractRequest].getErrorResponse(requestThrottleMs, e)
        if (response == null)
          new RequestChannel.Response(request, None, RequestChannel.CloseConnectionAction)
        else RequestChannel.Response(request, response)
      }
      error("Error when handling request %s".format(request.body[AbstractRequest]), e)
      if (mayThrottle)
        sendResponseMaybeThrottle(request, request.header.clientId, { requestThrottleMs =>
          requestChannel.sendResponse(createResponse(requestThrottleMs))
        })
      else
        sendResponseExemptThrottle(createResponse(0))
    }
  }

  private def sendResponseMaybeThrottle(request: RequestChannel.Request, createResponse: Int => AbstractResponse) {
    sendResponseMaybeThrottle(request, request.header.clientId, { requestThrottleMs =>
      sendResponse(request, createResponse(requestThrottleMs))
    })
  }

  private def sendResponseMaybeThrottle(request: RequestChannel.Request, clientId: String, sendResponseCallback: Int => Unit) {

    if (request.apiRemoteCompleteTimeNanos == -1) {
      request.apiRemoteCompleteTimeNanos = time.nanoseconds
    }
    val quotaSensors = quotas.request.getOrCreateQuotaSensors(request.session.sanitizedUser, clientId)
    def recordNetworkThreadTimeNanos(timeNanos: Long) {
      quotas.request.recordNoThrottle(quotaSensors, nanosToPercentage(timeNanos))
    }
    request.recordNetworkThreadTimeCallback = Some(recordNetworkThreadTimeNanos)

    quotas.request.recordAndThrottleOnQuotaViolation(
      quotaSensors,
      nanosToPercentage(request.requestThreadTimeNanos),
      sendResponseCallback)
  }

  private def sendResponseExemptThrottle(response: RequestChannel.Response) {
    sendResponseExemptThrottle(response.request, () => requestChannel.sendResponse(response))
  }

  private def sendResponseExemptThrottle(request: RequestChannel.Request, sendResponseCallback: () => Unit) {
    def recordNetworkThreadTimeNanos(timeNanos: Long) {
      quotas.request.recordExempt(nanosToPercentage(timeNanos))
    }
    request.recordNetworkThreadTimeCallback = Some(recordNetworkThreadTimeNanos)

    quotas.request.recordExempt(nanosToPercentage(request.requestThreadTimeNanos))
    sendResponseCallback()
  }

  private def sendResponse(request: RequestChannel.Request, response: AbstractResponse) {
    requestChannel.sendResponse(RequestChannel.Response(request, response))
  }

  private def nanosToPercentage(nanos: Long): Double = nanos * ClientQuotaManagerConfig.NanosToPercentagePerSecond

}
