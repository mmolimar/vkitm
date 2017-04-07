package com.github.mmolimar.kafka.vkitm.server

import java.io.File
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{Future => JFuture}

import com.github.mmolimar.kafka.vkitm.utils.Helpers._
import kafka.common._
import kafka.message.Message
import kafka.network.RequestChannel.Response
import kafka.network._
import kafka.utils.{Logging, SystemTime, ZkUtils}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors, SecurityProtocol}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.serialization.{ByteArraySerializer, Serializer}

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

  private val kafkaProducer = createNewProducer(Option(config.producerConfig))

  this.logIdent = "[VKitMApi-%d] ".format(config.serverConfig.brokerId)

  def handle(request: RequestChannel.Request) {
    try {
      trace("Handling request:%s from connection %s;securityProtocol:%s,principal:%s".
        format(request.requestDesc(true), request.connectionId, request.securityProtocol, request.session.principal))
      ApiKeys.forId(request.requestId) match {
        //by now, some produce ApiKeys are supported (just for producing messages)
        case ApiKeys.PRODUCE => handleProducerRequest(request)
        case ApiKeys.METADATA => handleTopicMetadataRequest(request)
        case ApiKeys.UPDATE_METADATA_KEY => handleUpdateMetadataRequest(request)
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
        def fromSuccess(rm: RecordMetadata) = {
          (topicPartition, new PartitionResponse(Errors.NONE.code, rm.offset(), rm.timestamp()))
        }

        def fromFailure(t: Throwable) = {
          (topicPartition, new PartitionResponse(Errors.forException(t).code, -1, Message.NoTimestamp))
        }

        for (f <- futures) yield {
          val p = Promise[(TopicPartition, PartitionResponse)]()
          f.onComplete {
            case Success(x) => p.success(fromSuccess(x))
            case Failure(t) => p.success(fromFailure(t))
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
        kafkaProducer.send(record).asScala

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
    val topicMetadata = getTopicMetadata(topics, request.securityProtocol, errorUnavailableEndpoints)

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

  private def getTopicMetadata(topics: Set[String], securityProtocol: SecurityProtocol, errorUnavailableEndpoints: Boolean): Seq[MetadataResponse.TopicMetadata] = {
    val topicResponses = metadataCache.getTopicMetadata(topics, securityProtocol, errorUnavailableEndpoints)
    if (topics.isEmpty || topicResponses.size == topics.size) {
      topicResponses
    } else {
      val nonExistentTopics = topics -- topicResponses.map(_.topic).toSet
      val responsesForNonExistentTopics = nonExistentTopics.map {
        topic =>
          //TODO send request to actual brokers
          new MetadataResponse.TopicMetadata(Errors.UNKNOWN_TOPIC_OR_PARTITION, topic, false,
            java.util.Collections.emptyList())
      }
      topicResponses ++ responsesForNonExistentTopics
    }
  }

  def close() {
    info("Shutdown complete.")
  }

  private def createNewProducer[K, V](props: Option[Properties] = None,
                                      acks: Int = 1,
                                      maxBlockMs: Long = 60 * 1000L,
                                      bufferSize: Long = 1024L * 1024L,
                                      retries: Int = 0,
                                      lingerMs: Long = 0,
                                      requestTimeoutMs: Long = 10 * 1024L,
                                      securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT,
                                      trustStoreFile: Option[File] = None,
                                      saslProperties: Option[Properties] = None,
                                      keySerializer: Serializer[K] = new ByteArraySerializer,
                                      valueSerializer: Serializer[V] = new ByteArraySerializer): KafkaProducer[K, V] = {

    val brokerList: String = metadataCache.getActualAliveBrokers.map {
      b =>
        b.endPoints.map {
          ep =>
            ep._2.host + ":" + ep._2.port
        }.mkString(",")
    }.mkString(",")
    val producerProps = props.getOrElse(new Properties)
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.ACKS_CONFIG, acks.toString)
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs.toString)
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferSize.toString)
    producerProps.put(ProducerConfig.RETRIES_CONFIG, retries.toString)
    producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs.toString)

    val defaultProps = Map(
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> "100",
      ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG -> "200",
      ProducerConfig.LINGER_MS_CONFIG -> lingerMs.toString
    )

    defaultProps.foreach {
      case (key, value) =>
        if (!producerProps.containsKey(key)) producerProps.put(key, value)
    }

    new KafkaProducer[K, V](producerProps, keySerializer, valueSerializer)
  }

}
