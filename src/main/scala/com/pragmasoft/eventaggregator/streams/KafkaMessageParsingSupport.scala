package com.pragmasoft.eventaggregator.streams

import akka.stream.scaladsl.Flow
import com.pragmasoft.eventaggregator.model.{EventKafkaLocation, KafkaAvroEvent}
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDecoder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.Try

trait KafkaMessageParsingSupport {
  self : LazyLogging =>

  def schemaRegistry: SchemaRegistryClient

  lazy val decoder = new KafkaAvroDecoder(schemaRegistry)

  lazy val parseKafkaMessage =
    Flow[ConsumerRecord[Array[Byte], Array[Byte]]]
      .map { kafkaMessage =>
        Try {
          logger.debug("Received message {}, decoding", kafkaMessage)

          val parsedMessage = decoder.fromBytes(kafkaMessage.value()).asInstanceOf[GenericRecord]

          KafkaAvroEvent(EventKafkaLocation.fromKafkaMessage(kafkaMessage), parsedMessage)
        }
      }
}
