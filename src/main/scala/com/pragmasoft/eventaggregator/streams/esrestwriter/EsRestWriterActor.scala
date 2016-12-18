package com.pragmasoft.eventaggregator.streams.esrestwriter

import akka.actor.{Actor, ActorLogging, Props}
import akka.event.LoggingReceive
import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter._
import com.pragmasoft.eventaggregator.model.KafkaAvroEvent
import io.searchbox.client.{JestClientFactory, JestResultHandler}
import io.searchbox.core.{DocumentResult, Index}
import org.apache.avro.generic.GenericRecord

object EsRestWriterActor {

  case class Write[T <: GenericRecord](event: KafkaAvroEvent[T])
  case class WriteResult[T <: GenericRecord](event: KafkaAvroEvent[T], succeeded: Boolean)

  def props(factory: JestClientFactory, elasticSearchIndex: () => String, headerDescriptor: EventHeaderDescriptor): Props =
    Props(new EsRestWriterActor(factory, elasticSearchIndex, headerDescriptor))

  case class EsDocumentInfo(documentType: String, maybeDocumentId: Option[String], source: String)

  def extractDocumentIndexingInfo(event: KafkaAvroEvent[GenericRecord])(implicit eventHeaderDescriptor: EventHeaderDescriptor): EsDocumentInfo = {
    val documentType = event.schemaName

    val maybeId = eventHeaderDescriptor.extractEventId(event.data)

    EsDocumentInfo(documentType, maybeId, kafkaAvroEventIndexable.json(event))
  }
}

class EsRestWriterActor(factory: JestClientFactory, val elasticSearchIndex: () => String, headerDescriptor: EventHeaderDescriptor) extends Actor with ActorLogging {
  import EsRestWriterActor._

  log.info("EsRestWriterActor: Initializing, creating ES REST Client")
  val client = factory.getObject()
  log.info("EsRestWriterActor: done")

  override def receive: Receive = LoggingReceive {
    case Write(event@KafkaAvroEvent(location, data)) =>
      log.debug("Asked to write to ES a new monitored event {}", event)
      val documentInfo = extractDocumentIndexingInfo(event)(headerDescriptor)

      val esIndex = elasticSearchIndex()
      val indexWithoutId = new Index.Builder(documentInfo.source).`type`(documentInfo.documentType).index(esIndex)
      log.debug("Indexing document {} on index '{}'", documentInfo, esIndex)

      val index = documentInfo.maybeDocumentId.fold(indexWithoutId){ documentId =>
        indexWithoutId.id(documentId)
      }.build()

      log.debug("Calling async indexing operation")
      val writerCoordinator = sender()
      client.executeAsync(
        index,
        new JestResultHandler[DocumentResult] {
          override def completed(result: DocumentResult): Unit = {
            val succeeded = result.isSucceeded
            if(succeeded)
              log.debug("Indexing of event {} succeeeded", event)
            else
              log.warning("Indexing of event {} failed with response code {}", event, result.getResponseCode)

            writerCoordinator ! WriteResult(event, succeeded)
          }

          override def failed(ex: Exception): Unit = {
            log.warning("Error trying to write into ES {}", ex)
            writerCoordinator ! WriteResult(event, false)
          }
        }
      )
  }

  override def postStop() = {
    log.info("Shutting down client")
    client.shutdownClient()
  }
}
