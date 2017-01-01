package com.pragmasoft.eventaggregator

import akka.actor.ActorSystem
import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.EventHeaderDescriptor
import com.pragmasoft.eventaggregator.http.HttpServer
import com.pragmasoft.eventaggregator.streams.{KafkaPublisherConfig, KafkaToNativeElasticsearchAggregatorFLow, KafkaToRestElasticsearchAggregatorFLow}
import com.sksamuel.elastic4s.ElasticClient
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.commons.lang3.RandomStringUtils.randomAlphabetic
import org.elasticsearch.common.settings.ImmutableSettings


case class KafkaConfig(kafkaBootstrapBrokers: String = "",
                       consumerGroup: String = randomAlphabetic(10),
                       readFromBeginning: Boolean = false,
                       topicRegex: String = ".+",
                       schemaRegistryUrl: String = ""
                      )

case class EsConfig(esHost: String = "",
                    esPort: Int = 9300,
                    useHttp: Boolean = false)

case class EventsSyntaxConfig(eventIdPath: String = "header/eventId",
                              eventTsPath: String = "header/timestamp")

case class EventAggregatorArgs(
                                kafkaConfig: KafkaConfig = KafkaConfig(),
                                esConfig: EsConfig = EsConfig(),
                                eventsSyntaxConfig: EventsSyntaxConfig = EventsSyntaxConfig()
                              )

object EventAggregatorArgs {
  def withDefaultsFromConfig(config: Config): EventAggregatorArgs = EventAggregatorArgs()
}

class EventAggregator(args: EventAggregatorArgs, config: Config, schemaRegistry: SchemaRegistryClient) extends LazyLogging {
  val kafkaConfig = KafkaPublisherConfig(
    reactiveKafkaDispatcher = config.getString("kafka.actor_dispatcher_name"),
    bootstrapBrokers = args.kafkaConfig.kafkaBootstrapBrokers,
    topicRegex = args.kafkaConfig.topicRegex,
    groupId = args.kafkaConfig.consumerGroup,
    readFromBeginning = args.kafkaConfig.readFromBeginning
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

    val esIndexPrefix = config.getString("elasticsearch.indexPrefix")
    implicit val actorSystem = ActorSystem("KafkaEventMonitorApp")
    val aggregator =
      if(args.esConfig.useHttp) {
        new KafkaToNativeElasticsearchAggregatorFLow(
          kafkaConfig,
          esIndexPrefix,
          actorSystem,
          schemaRegistry,
          EventHeaderDescriptor(Some(args.eventsSyntaxConfig.eventIdPath), Some(args.eventsSyntaxConfig.eventIdPath)),
          ElasticClient.remote(args.esConfig.esHost, args.esConfig.esPort)
        )
      } else {
        new KafkaToRestElasticsearchAggregatorFLow(
          kafkaConfig,
          esIndexPrefix,
          esConnectionUrl,
          actorSystem,
          schemaRegistry,
          EventHeaderDescriptor(Some(args.eventsSyntaxConfig.eventIdPath), Some(args.eventsSyntaxConfig.eventIdPath))
        )
    }

    logger.info(s"KafkaEventMonitorApp starting, connecting to es at '$esConnectionUrl' with index prefix $esIndexPrefix")

    new HttpServer(config).start()
    aggregator.startFlow()

    logger.info("KafkaEventMonitorApp started")

    sys.addShutdownHook {
      logger.info("Shutting down actor system")
      actorSystem.terminate()
      logger.info(".. done")
    }
  }
}

object EventAggregatorApp extends App {
  private val config = ConfigFactory.load()

  val cmdLineArgs = EventAggregatorArgs.withDefaultsFromConfig(config)
  val appInstance = new EventAggregator(cmdLineArgs, config, new CachedSchemaRegistryClient(cmdLineArgs.kafkaConfig.schemaRegistryUrl, 100))

  appInstance.run()

}
