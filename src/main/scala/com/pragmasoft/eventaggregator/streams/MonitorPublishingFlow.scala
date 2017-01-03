package com.pragmasoft.eventaggregator.streams

import akka.event.Logging
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Attributes}
import com.pragmasoft.eventaggregator.ActorSystemProvider
import com.pragmasoft.eventaggregator.model.KafkaAvroEvent
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord

trait MonitorPublishingFlow[Materializer] extends KafkaMessageParsingSupport with AkkaStreamFlowOperations {
  self:
    ActorSystemProvider with
    LazyLogging with
    SourceProvider[ConsumerRecord[Array[Byte], Array[Byte]], _] with
    SinkProvider[KafkaAvroEvent[GenericRecord], Materializer] =>

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(actorSystem).withSupervisionStrategy(alwaysResume("Error trying to save topics content into elastic search")))

  def startFlow(): Materializer = {
    source.log("received-message").withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
      .via(parseKafkaMessage)
      .via(filterAndLogFailures[KafkaAvroEvent[GenericRecord]]("Unable to deserialize message, dropping it"))
    .runWith(sink)
  }

}
