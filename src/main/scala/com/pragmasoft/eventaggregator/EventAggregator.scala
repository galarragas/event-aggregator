package com.pragmasoft.eventaggregator

import akka.actor.ActorSystem
import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.EventHeaderDescriptor
import com.pragmasoft.eventaggregator.http.HttpServer
import com.pragmasoft.eventaggregator.streams.{KafkaPublisherConfig, KafkaToNativeElasticsearchAggregatorFLow, KafkaToRestElasticsearchAggregatorFLow}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.elasticsearch.common.settings.ImmutableSettings

case class
case class EventAggregatorArgs(
                                kafkaBootstrapBrokers: String = "",
                                readFromBeginning: Boolean = false,
                                topicRegex: String = ".+",

                              )

object EventAggregatorArgs {
  def withDefaultsFromConfig(config: Config): EventAggregatorArgs
}

class EventAggregator(config: Config, schemaRegistry: SchemaRegistryClient) extends LazyLogging {
  val kafkaConfig = KafkaPublisherConfig(
    reactiveKafkaDispatcher = config.getString("kafka.actor_dispatcher_name"),
    bootstrapBrokers = config.getString("kafka.bootstrap_broker_list"),
    topicRegex = config.getString("kafka.topics_regex"),
    groupId = config.getString("kafka.consumer_group"),
    readFromBeginning = config.getBoolean("kafka.read_from_beginning")
  )

  def run() {
    val elasticsearchClientSettings =
      ImmutableSettings
        .settingsBuilder
        .put("client.transport.ignore_cluster_name", true)
        .put("client.transport.sniff", true)
        .build

    val esHost = config.getString("elasticsearch.host")
    val esPort = config.getInt("elasticsearch.port")
    val esConnectionUrl = s"http://$esHost:$esPort"

    val eventIdPath = "header/eventId"
    val eventTsPath = "header/timestamp"

    val esIndexPrefix = config.getString("elasticsearch.indexPrefix")
    implicit val actorSystem = ActorSystem("KafkaEventMonitorApp")
    val eventMonitorPublisher = new KafkaToRestElasticsearchAggregatorFLow(
      kafkaConfig,
      esIndexPrefix,
      esConnectionUrl,
      actorSystem,
      schemaRegistry,
      EventHeaderDescriptor(Some(eventIdPath), Some(eventIdPath))
    )

    logger.info(s"KafkaEventMonitorApp starting, connecting to es at '$esConnectionUrl' with index prefix $esIndexPrefix")

    new HttpServer(config).start()
    eventMonitorPublisher.startFlow()

    logger.info("KafkaEventMonitorApp started")

    sys.addShutdownHook {
      logger.info("Shutting down actor system")
      actorSystem.terminate()
      logger.info(".. done")
    }
  }
}

object KafkaEventMonitorApp extends App {
  val appInstance = new EventAggregator(ConfigFactory.load(), new CachedSchemaRegistryClient())

  appInstance.run()

}
