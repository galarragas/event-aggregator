package com.pragmasoft.eventaggregator.streams

import akka.NotUsed
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import com.pragmasoft.eventaggregator.ActorSystemProvider
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

case class KafkaPublisherConfig(reactiveKafkaDispatcher: String, bootstrapBrokers: String, topicRegex: String, groupId: String, readFromBeginning: Boolean)

trait SourceProvider[T, Mat] {
  def source: Source[T, Mat]
}

trait KafkaSourceProvider extends SourceProvider[ConsumerRecord[Array[Byte], Array[Byte]], Control] with LazyLogging {
  self: ActorSystemProvider =>

  def kafkaConfig: KafkaPublisherConfig

  lazy val consumerProperties = {
    ConsumerSettings(actorSystem, new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(kafkaConfig.bootstrapBrokers)
      .withGroupId(kafkaConfig.groupId)
      .withProperty(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        if (kafkaConfig.readFromBeginning)
          "earliest"
        else
          "latest"
      )
      .withProperty(
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        "true"
      )
      .withDispatcher(kafkaConfig.reactiveKafkaDispatcher)
  }

  override lazy val source: Source[ConsumerRecord[Array[Byte], Array[Byte]], Control] =
    Consumer.atMostOnceSource(consumerProperties, Subscriptions.topics(kafkaConfig.topicRegex))
}
