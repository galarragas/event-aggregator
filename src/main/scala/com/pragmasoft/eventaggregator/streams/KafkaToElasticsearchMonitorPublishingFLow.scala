package com.pragmasoft.eventaggregator.streams

import akka.NotUsed
import akka.actor.ActorSystem
import com.pragmasoft.eventaggregator.ActorSystemProvider
import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.EventHeaderDescriptor
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

class KafkaToElasticsearchMonitorPublishingFLow(
    override val kafkaConfig: KafkaPublisherConfig,
    override val elasticSearchIndexPrefix: String,
    override val elasticSearchConnectionUrl: String,
    override implicit val actorSystem: ActorSystem,
    override val schemaRegistry: SchemaRegistryClient,
    override val headerDescriptor: EventHeaderDescriptor
  )
  extends MonitorPublishingFlow[NotUsed]
  with KafkaSourceProvider
  with RestElasticsearchEventSinkProvider
  with ActorSystemProvider
  with DayPartitionedElasticSearchIndexNameProvider
  with LazyLogging {
}
