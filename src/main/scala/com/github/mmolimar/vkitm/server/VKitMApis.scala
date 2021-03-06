package com.github.mmolimar.vkitm.server

import java.io.IOException
import java.nio.ByteBuffer
import java.util
import java.util.UUID
import java.util.concurrent.{Future => JFuture}

import com.github.mmolimar.vkitm.common.cache.{Cache, ClientProducerRequest, NetworkClientRequest}
import com.github.mmolimar.vkitm.utils.Helpers.JFutureHelpers
import kafka.common._
import kafka.message.Message
import kafka.network.RequestChannel.Response
import kafka.network._
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.clients.{ClientRequest, ClientResponse}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{ApiException, NetworkException}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors, Protocol, SecurityProtocol}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.MetadataResponse.{PartitionMetadata, TopicMetadata}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._

import scala.collection.JavaConverters._
import scala.collection._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Promise, Future => SFuture}
import scala.util.{Failure, Success}

class VKitMApis(val requestChannel: RequestChannel,
                val zkUtils: ZkUtils,
                val config: VKitMConfig,
                val metadataCache: FakedMetadataCache,
                val metrics: Metrics,
                val clusterId: String) extends Logging {

  private val producerCache = Cache.forProducers()
  private val clientCache = Cache.forClients()
  private val consumerConfig = KafkaConfig.fromProps(config.consumerProps)

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
        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request)
        case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request)
        case ApiKeys.GROUP_COORDINATOR => handleGroupCoordinatorRequest(request)
        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request)
        case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request)
        case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request)
        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case requestId => throw new KafkaException("Unknown api code " + requestId)
      }
    } catch {
      case e: Throwable =>
        if (request.requestObj != null) {
          request.requestObj.handleError(e, requestChannel, request)
          error("Error when handling request %s".format(request.requestObj), e)
        } else {
          val response = request.body.getErrorResponse(request.header.apiVersion, e)
          val respHeader = new ResponseHeader(request.header.correlationId)

          if (response == null)
            requestChannel.closeConnection(request.processor, request)
          else
            requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, respHeader, response)))

          error("Error when handling request %s".format(request.body), e)
        }
    } finally {
      request.apiLocalCompleteTimeMs = SystemTime.milliseconds
    }
  }

  def handleProducerRequest(request: RequestChannel.Request) {
    val produceRequest = request.body.asInstanceOf[ProduceRequest]

    def sendRecord(topicPartition: TopicPartition, buffer: ByteBuffer): Seq[SFuture[(TopicPartition, PartitionResponse)]] = {

      def transform(futures: Seq[SFuture[RecordMetadata]]) = {

        implicit def makeResponse(p: AnyRef): (TopicPartition, PartitionResponse) = p match {
          case rm: RecordMetadata => (topicPartition, new PartitionResponse(Errors.NONE.code, rm.offset(), rm.timestamp()))
          case t: Throwable => {
            val cause = if (t.getCause != null) t.getCause else t
            (topicPartition, new PartitionResponse(Errors.forException(cause).code, -1, Message.NoTimestamp))
          }
        }

        for (f <- futures) yield {
          val p = Promise[(TopicPartition, PartitionResponse)]()
          f.onComplete {
            //all is mapped as a success response, then make the custom partition response
            case Success(s) => p.success(s)
            case Failure(f) => p.success(f)
          }
          p.future
        }
      }

      val futures = MemoryRecords.readableRecords(buffer).asScala.map { rm =>
        val key = {
          rm.record().key() match {
            case null => null
            case _ => rm.record().key().array()
          }
        }
        val value = {
          rm.record().value() match {
            case null => null
            case _ => rm.record().value().array().slice(rm.record().value().arrayOffset(), rm.record().value().array().length)
          }
        }
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topicPartition.topic(), key, value)
        val entry = ClientProducerRequest(request.header.clientId, getBrokerList, produceRequest.acks)(config.producerProps)
        producerCache.getAndMaybePut(entry).send(record).asScala

      }.toSeq

      transform(futures)
    }

    val result: Seq[SFuture[(TopicPartition, PartitionResponse)]] = produceRequest.partitionRecords.asScala.map {
      case (topicPartition, buffer) => sendRecord(topicPartition, buffer)
    }.flatten.toSeq

    SFuture.sequence(result).onComplete { r =>
      val responsesByTopicPartition: Map[TopicPartition, PartitionResponse] = r.get.map { item =>
        item._1 -> item._2
      }.toMap

      val respBody = request.header.apiVersion match {
        case 0 => new ProduceResponse(responsesByTopicPartition.asJava)
        case version@(1 | 2) => new ProduceResponse(responsesByTopicPartition.asJava, 0, version)
        case version => throw new IllegalArgumentException(s"Version `$version` of ProduceRequest is not handled. Code must be updated.")
      }
      val respHeader = new ResponseHeader(request.header.correlationId)
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, respHeader, respBody)))
      produceRequest.clearPartitionRecords()
    }

  }

  def handleFetchRequest(request: RequestChannel.Request) {
    val sFetchRequest = request.requestObj.asInstanceOf[kafka.api.FetchRequest]
    val fetchData: Map[TopicPartition, FetchRequest.PartitionData] = sFetchRequest.requestInfo.map { ri =>
      (new TopicPartition(ri._1.topic, ri._1.partition), new FetchRequest.PartitionData(ri._2.offset, sFetchRequest.maxBytes))
    }.toMap

    val jFetchRequest = new FetchRequest(sFetchRequest.maxWait,
      sFetchRequest.minBytes,
      sFetchRequest.maxBytes,
      new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData](fetchData.asJava))

    val header = new RequestHeader(ApiKeys.FETCH.id, sFetchRequest.versionId, sFetchRequest.clientId, sFetchRequest.correlationId)
    val ncr = NetworkClientRequest(sFetchRequest.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(header, request,
      jFetchRequest, ncr, request.connectionId,
      sFetchRequest.correlationId, response => new FetchResponse(response.responseBody()))

  }

  def handleListOffsetRequest(request: RequestChannel.Request) {
    val listOffsetRequest = request.body.asInstanceOf[ListOffsetRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      listOffsetRequest, ncr, request.connectionId,
      request.header.correlationId, response => new ListOffsetResponse(response.responseBody()))

  }

  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = request.body.asInstanceOf[MetadataRequest]
    val requestVersion = request.header.apiVersion()

    val topics =
      if (requestVersion == 0) {
        if (metadataRequest.topics() == null || metadataRequest.topics().isEmpty)
          metadataCache.getAllTopics()
        else
          metadataRequest.topics.asScala.toSet
      } else {
        if (metadataRequest.isAllTopics)
          metadataCache.getAllTopics()
        else
          metadataRequest.topics.asScala.toSet
      }

    val errorUnavailableEndpoints = requestVersion == 0
    val topicMetadata = getTopicMetadata(request, topics, request.securityProtocol, errorUnavailableEndpoints)

    val brokers = metadataCache.getVirtualAliveBrokers

    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(topicMetadata.mkString(","),
      brokers.mkString(","), request.header.correlationId, request.header.clientId))

    val responseHeader = new ResponseHeader(request.header.correlationId)

    val responseBody = new MetadataResponse(
      brokers.map(_.getNode(request.securityProtocol)).asJava,
      clusterId,
      metadataCache.getControllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID),
      topicMetadata.asJava,
      requestVersion
    )
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))

  }

  def handleUpdateMetadataRequest(request: RequestChannel.Request) {
    val correlationId = request.header.correlationId

    //always fine, the metadata is updated when starting the app and by the zkNode change listeners
    val updateMetadataResponse = new UpdateMetadataResponse(Errors.NONE.code)

    val responseHeader = new ResponseHeader(correlationId)
    requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, responseHeader, updateMetadataResponse)))

  }

  def handleOffsetCommitRequest(request: RequestChannel.Request) {
    val offsetCommitRequest = request.body.asInstanceOf[OffsetCommitRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      offsetCommitRequest, ncr, request.connectionId,
      request.header.correlationId, response => new OffsetCommitResponse(response.responseBody()))

  }

  def handleOffsetFetchRequest(request: RequestChannel.Request) {
    val offsetFetchRequest = request.body.asInstanceOf[OffsetFetchRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      offsetFetchRequest, ncr, request.connectionId,
      request.header.correlationId, response => new OffsetFetchResponse(response.responseBody()))

  }

  def handleGroupCoordinatorRequest(request: RequestChannel.Request) {
    val groupCoordinatorRequest = request.body.asInstanceOf[GroupCoordinatorRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      groupCoordinatorRequest, ncr, request.connectionId,
      request.header.correlationId,
      response => {
        val coordinatorResponse = new GroupCoordinatorResponse(response.responseBody())
        new GroupCoordinatorResponse(coordinatorResponse.errorCode(), metadataCache.getVirtualAliveNodes.head)
      },
      Option(t => new GroupCoordinatorResponse(Errors.forException(t).code, metadataCache.getVirtualAliveNodes.head)))
  }

  def handleJoinGroupRequest(request: RequestChannel.Request) {
    val joinGroupRequest = request.body.asInstanceOf[JoinGroupRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId + UUID.randomUUID())(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      joinGroupRequest, ncr, request.connectionId,
      request.header.correlationId, response => new JoinGroupResponse(response.responseBody()))

  }

  def handleHeartbeatRequest(request: RequestChannel.Request) {
    val heartbeatRequest = request.body.asInstanceOf[HeartbeatRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      heartbeatRequest, ncr, request.connectionId,
      request.header.correlationId, response => new HeartbeatResponse(response.responseBody()))
  }

  def handleLeaveGroupRequest(request: RequestChannel.Request) {
    val leaveGroupRequest = request.body.asInstanceOf[LeaveGroupRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      leaveGroupRequest, ncr, request.connectionId,
      request.header.correlationId, response => new LeaveGroupResponse(response.responseBody()))
  }

  def handleSyncGroupRequest(request: RequestChannel.Request) {
    val syncGroupRequest = request.body.asInstanceOf[SyncGroupRequest]
    val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

    sendNetworkClientRequest(request.header, request,
      syncGroupRequest, ncr, request.connectionId,
      request.header.correlationId, response => new SyncGroupResponse(response.responseBody()))

  }

  def handleApiVersionsRequest(request: RequestChannel.Request) {
    val responseHeader = new ResponseHeader(request.header.correlationId)
    val responseBody = if (Protocol.apiVersionSupported(ApiKeys.API_VERSIONS.id, request.header.apiVersion))
      ApiVersionsResponse.apiVersionsResponse
    else
      ApiVersionsResponse.fromError(Errors.UNSUPPORTED_VERSION)
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
  }

  private def getTopicMetadata(request: RequestChannel.Request, topics: Set[String], securityProtocol: SecurityProtocol, errorUnavailableEndpoints: Boolean): Seq[MetadataResponse.TopicMetadata] = {
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

      import NetworkClientBlockingOps._

      //must request update to the brokers
      val time = new org.apache.kafka.common.utils.SystemTime()
      val node = metadataCache.getActualAliveNodes.head
      val send = new RequestSend(node.idString, request.header, request.body.toStruct)
      val clientRequest = new ClientRequest(time.milliseconds, true, send, null)
      val ncr = NetworkClientRequest(request.header.clientId + "-" + request.requestId)(metadataCache.getMetadataUpdater, consumerConfig, metrics)

      val networkClient = clientCache.getAndMaybePut(ncr)
      try {
        if (!networkClient.blockingReady(node, config.serverConfig.requestTimeoutMs.longValue)(time)) {
          throw new NetworkException(s"Failed to connect")
        }

        val response = clientCache.getAndMaybePut(ncr).blockingSendAndReceive(clientRequest)(time)
        val metadataResponse = new MetadataResponse(response.responseBody())

        metadataResponse.topicMetadata.asScala.map { tm =>
          new TopicMetadata(tm.error(), tm.topic(), tm.isInternal,
            tm.partitionMetadata.asScala.map(fakePartitionMetadata(_)).asJava)
        }.toSeq

      } catch {
        case ioe: IOException => errorResponse(new NetworkException(ioe))
        case ae: ApiException => errorResponse(ae)
      }
    }
  }

  def close() {
    info("Shutdown complete.")
  }

  private def sendNetworkClientRequest(header: RequestHeader,
                                       request: RequestChannel.Request,
                                       jRequest: AbstractRequest,
                                       ncr: NetworkClientRequest,
                                       connectionId: String,
                                       correlationId: Int,
                                       buildResponse: ClientResponse => AbstractRequestResponse,
                                       errorResponse: Option[Throwable => AbstractRequestResponse] = None) {

    val time = new org.apache.kafka.common.utils.SystemTime()
    val node = metadataCache.getActualAliveNodes.head
    val send = new RequestSend(node.idString, header, jRequest.toStruct)
    val clientRequest = new ClientRequest(time.milliseconds, true, send, null)

    val networkClient = clientCache.getAndMaybePut(ncr)

    val responseHeader = new ResponseHeader(correlationId)
    val responseBody = {
      def resultException(t: Throwable) = {
        jRequest.getErrorResponse(header.apiVersion, t)
      }

      try {
        import NetworkClientBlockingOps._

        if (!networkClient.blockingReady(node, config.serverConfig.requestTimeoutMs.longValue)(time)) {
          throw new NetworkException(s"Failed to connect")
        }

        val response = clientCache.getAndMaybePut(ncr).blockingSendAndReceive(clientRequest)(time)
        buildResponse(response)

      } catch {
        case ioe: IOException => errorResponse.getOrElse(resultException(_))(new NetworkException(ioe))
        case ae: ApiException => errorResponse.getOrElse(resultException(_))(ae)
      }
    }

    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(connectionId, responseHeader, responseBody)))
  }

  private def getBrokerList() = {
    metadataCache.getActualAliveBrokers.map {
      _.endPoints.map {
        ep =>
          ep._2.host + ":" + ep._2.port
      }.mkString(",")
    }.mkString(",")
  }

}
