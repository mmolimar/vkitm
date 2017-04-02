package com.github.mmolimar.kafka.vkitm.server

import kafka.common._
import kafka.network.RequestChannel.Response
import kafka.network._
import kafka.utils.{Logging, SystemTime, ZkUtils}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._

class VKitMApis(val requestChannel: RequestChannel,
                val zkUtils: ZkUtils,
                val config: VKitMConfig,
                val metrics: Metrics,
                val clusterId: String) extends Logging {

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

          /* If request doesn't have a default error response, we just close the connection.
             For example, when produce request has acks set to 0 */
          if (response == null)
            requestChannel.closeConnection(request.processor, request)
          else
            requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, respHeader, response)))

          error("Error when handling request %s".format(request.body), e)
        }
    } finally
      request.apiLocalCompleteTimeMs = SystemTime.milliseconds
  }

  def handleProducerRequest(request: RequestChannel.Request) {
    val produceRequest = request.body.asInstanceOf[ProduceRequest]

    //TODO
    produceRequest.clearPartitionRecords()
  }


  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = request.body.asInstanceOf[MetadataRequest]

    //TODO
  }

  def handleUpdateMetadataRequest(request: RequestChannel.Request) {
    val correlationId = request.header.correlationId
    val updateMetadataRequest = request.body.asInstanceOf[UpdateMetadataRequest]

    //TODO
    val updateMetadataResponse = new UpdateMetadataResponse(Errors.NONE.code)

    val responseHeader = new ResponseHeader(correlationId)
    requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, responseHeader, updateMetadataResponse)))
  }

  def close() {
    info("Shutdown complete.")
  }

}
