package com.pragmasoft.eventaggregator

import akka.actor.ActorSystem
import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.EventHeaderDescriptor
import com.pragmasoft.eventaggregator.http.HttpServer
import com.pragmasoft.eventaggregator.streams.{KafkaPublisherConfig, KafkaToNativeElasticsearchAggregatorFLow, KafkaToRestElasticsearchAggregatorFLow}
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.elasticsearch.common.settings.Settings


case class KafkaConfig(kafkaBootstrapBrokers: String = "",
                       consumerGroup: String = "kafka-event-aggregator",
                       readFromBeginning: Boolean = false,
                       topicRegex: String = ".+",
                       schemaRegistryUrl: String = ""
                      )

case class EsConfig(esHost: String = "",
                    esPort: Int = 9300,
                    useHttp: Boolean = false,
                    indexPrefix: String = "events"
                   )

case class EventsSyntaxConfig(eventIdPath: String = "header/eventId",
                              eventTsPath: String = "header/timestamp")

case class EventAggregatorArgs(
                                kafkaConfig: KafkaConfig = KafkaConfig(),
                                esConfig: EsConfig = EsConfig(),
                                eventsSyntaxConfig: EventsSyntaxConfig = EventsSyntaxConfig(),
                                httpPort: Int = 8080
                              )


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
      Settings
        .settingsBuilder
        .put("client.transport.ignore_cluster_name", true)
        .put("client.transport.sniff", true)
        .build

    val esIndexPrefix = args.esConfig.indexPrefix
    implicit val actorSystem = ActorSystem("KafkaEventAggregator")
    val aggregator =
      if(args.esConfig.useHttp) {
        new KafkaToNativeElasticsearchAggregatorFLow(
          kafkaConfig,
          esIndexPrefix,
          actorSystem,
          schemaRegistry,
          EventHeaderDescriptor(Some(args.eventsSyntaxConfig.eventIdPath), Some(args.eventsSyntaxConfig.eventIdPath)),
          ElasticClient.transport(elasticsearchClientSettings, ElasticsearchClientUri(args.esConfig.esHost, args.esConfig.esPort))
        )
      } else {
        val esConnectionUrl = s"http://${args.esConfig.esHost}:${args.esConfig.esPort}"

        new KafkaToRestElasticsearchAggregatorFLow(
          kafkaConfig,
          esIndexPrefix,
          esConnectionUrl,
          actorSystem,
          schemaRegistry,
          EventHeaderDescriptor(Some(args.eventsSyntaxConfig.eventIdPath), Some(args.eventsSyntaxConfig.eventIdPath))
        )
    }

    logger.info(s"KafkaEventMonitorApp starting, connecting to es using HTTP? ${args.esConfig.useHttp} at '${args.esConfig.esHost}'-'${args.esConfig.esPort}' with index prefix $esIndexPrefix")

    new HttpServer(args.httpPort).start()
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

  val cmdLineArgs = EventAggregatorArgs()
  val appInstance = new EventAggregator(cmdLineArgs, config, new CachedSchemaRegistryClient(cmdLineArgs.kafkaConfig.schemaRegistryUrl, 100))

  appInstance.run()

}
