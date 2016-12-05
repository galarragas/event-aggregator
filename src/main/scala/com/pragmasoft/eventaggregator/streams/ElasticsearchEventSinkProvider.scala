package com.pragmasoft.eventaggregator.streams

import akka.NotUsed
import akka.stream.actor.ActorSubscriber
import akka.stream.scaladsl.Sink
import com.pragmasoft.eventaggregator.ActorSystemProvider
import com.pragmasoft.eventaggregator.model.KafkaAvroEvent
import com.pragmasoft.eventaggregator.streams.esrestwriter.EsRestActorPoolSubscriber
import com.sksamuel.elastic4s.streams.ReactiveElastic.ReactiveElastic
import com.sksamuel.elastic4s.streams.{RequestBuilder, ResponseListener}
import com.sksamuel.elastic4s.{BulkCompatibleDefinition, ElasticClient, ElasticDsl}
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.generic.GenericRecord
import org.elasticsearch.action.bulk.BulkItemResponse

import scala.concurrent.duration._

trait SinkProvider[T, Mat] {
  def sink: Sink[T, Mat]
}

trait ElasticsearchEventSinkProvider extends SinkProvider[KafkaAvroEvent[GenericRecord], NotUsed] {

  self: ActorSystemProvider with LazyLogging with ElasticSearchIndexNameProvider =>

  def elasticSearchClient: ElasticClient
  def batchSize: Int = 100
  def concurrentRequests: Int = 5
  def flushInterval: FiniteDuration = 10.seconds

  implicit val builder = new RequestBuilder[KafkaAvroEvent[GenericRecord]] {
    import ElasticDsl._
    import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter._

    def request(event: KafkaAvroEvent[GenericRecord]): BulkCompatibleDefinition = {
      val documentType = event.schemaName

      val maybeId = for {
        field <- Option(event.data.getSchema.getField("header"))  //this is to prevent a nastly NPE when accessing the field as the second line does
        header <- Option(event.data.get("header")).map(_.asInstanceOf[GenericRecord])
        id <- Option(header.get("eventId").toString)
      } yield id.toString

      logger.debug("Indexing document {} into: '{}/{}' with ID {}", event, elasticSearchIndex, documentType, maybeId)

      val withoutIndex = index into elasticSearchIndex / documentType source event

      maybeId.fold(withoutIndex) { docId => withoutIndex id docId }
    }
  }

  override def sink: Sink[KafkaAvroEvent[GenericRecord], NotUsed] = {
    Sink.fromSubscriber(new ReactiveElastic(elasticSearchClient).subscriber[KafkaAvroEvent[GenericRecord]](
      batchSize = batchSize,
      concurrentRequests = concurrentRequests,
      flushInterval = Some(flushInterval),
      completionFn = { () => logger.info("Subcriber flow completed") },
      errorFn = { throwable => logger.error("Error during monitoring flow", throwable) },
      listener = new ResponseListener {
        override def onAck(resp: BulkItemResponse): Unit = {
          if (resp.isFailed) {
            logger.error("Received failed ack for bulk indexing failure: {}", resp.getFailure.getMessage)
          } else {
            logger.debug("Received sucessful ack for bulk indexing index {}, id: {}", resp.getIndex, resp.getId)
          }
        }
      }
    ))
  }

}

trait RestElasticsearchEventSinkProvider extends SinkProvider[KafkaAvroEvent[GenericRecord], NotUsed] {
  self: ActorSystemProvider with LazyLogging with ElasticSearchIndexNameProvider =>

  def elasticSearchConnectionUrl: String
  def elasticSearchIndexPrefix: String

  def calculateIndexName = () => elasticSearchIndex

  override lazy val sink: Sink[KafkaAvroEvent[GenericRecord], NotUsed] =
    Sink.fromSubscriber(
      ActorSubscriber[KafkaAvroEvent[GenericRecord]](
        actorSystem.actorOf(EsRestActorPoolSubscriber.props(10, 15, calculateIndexName, elasticSearchConnectionUrl))
      )
    )
}