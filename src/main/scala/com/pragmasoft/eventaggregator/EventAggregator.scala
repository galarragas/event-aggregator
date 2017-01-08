package com.pragmasoft.eventaggregator

import akka.Done
import akka.actor.ActorSystem
import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.EventHeaderDescriptor
import com.pragmasoft.eventaggregator.http.HttpServer
import com.pragmasoft.eventaggregator.streams.{KafkaPublisherConfig, KafkaToNativeElasticsearchAggregatorFLow, KafkaToRestElasticsearchAggregatorFLow}
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings

import scala.concurrent.Future
import scala.util.{Failure, Success}


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

case class EventsSyntaxConfig(eventIdPath: Option[String] = None,
                              eventTsPath: Option[String] = None)

case class EventAggregatorArgs(
                                kafkaConfig: KafkaConfig = KafkaConfig(),
                                esConfig: EsConfig = EsConfig(),
                                eventsSyntaxConfig: EventsSyntaxConfig = EventsSyntaxConfig(),
                                httpPort: Int = 8080
                              )
class EventAggregator(args: EventAggregatorArgs, config: Config, schemaRegistry: SchemaRegistryClient, localClientForTest: Option[Client] = None)(implicit actorSystem: ActorSystem) extends LazyLogging {
  private lazy val aggregator = {
    val kafkaConfig = KafkaPublisherConfig(
      reactiveKafkaDispatcher = config.getString("kafka.actor_dispatcher_name"),
      bootstrapBrokers = args.kafkaConfig.kafkaBootstrapBrokers,
      topicRegex = args.kafkaConfig.topicRegex,
      groupId = args.kafkaConfig.consumerGroup,
      readFromBeginning = args.kafkaConfig.readFromBeginning
    )

    val eventHeaderDescriptor = EventHeaderDescriptor(args.eventsSyntaxConfig.eventIdPath, args.eventsSyntaxConfig.eventTsPath)

    if (args.esConfig.useHttp) {
      new KafkaToRestElasticsearchAggregatorFLow(
        kafkaConfig = kafkaConfig,
        elasticSearchIndexPrefix = args.esConfig.indexPrefix,
        elasticSearchConnectionUrl = s"http://${args.esConfig.esHost}:${args.esConfig.esPort}",
        actorSystem = actorSystem,
        schemaRegistry = schemaRegistry,
        headerDescriptor = eventHeaderDescriptor,
        esWriterActorDispatcher = config.getString("elasticsearch.actor_dispatcher_name")
      )
    } else {
      val client = localClientForTest.fold {
        val elasticsearchClientSettings =
          Settings
            .settingsBuilder
            .put("client.transport.ignore_cluster_name", true)
            .put("client.transport.sniff", true)
            .build

        ElasticClient.transport(elasticsearchClientSettings, ElasticsearchClientUri(args.esConfig.esHost, args.esConfig.esPort))
      }(ElasticClient.fromClient)

      new KafkaToNativeElasticsearchAggregatorFLow(
        kafkaConfig = kafkaConfig,
        elasticSearchIndexPrefix = args.esConfig.indexPrefix,
        actorSystem = actorSystem,
        schemaRegistry = schemaRegistry,
        headerDescriptor = eventHeaderDescriptor,
        elasticSearchClient = client
      )
    }
  }


  def run(): Future[Done] = {
    logger.info(s"KafkaEventMonitorApp starting, connecting to es using HTTP? ${args.esConfig.useHttp}, " +
      s"at '${args.esConfig.esHost}'-'${args.esConfig.esPort}' with index prefix ${args.esConfig.indexPrefix}")

    new HttpServer(args.httpPort).start()
    val (completionFuture, _) = aggregator.startFlow()

    logger.info("KafkaEventMonitorApp started")

    completionFuture
  }
}

object EventAggregatorApp extends App with LazyLogging {
  private val config = ConfigFactory.load()

  implicit val actorSystem = ActorSystem("KafkaEventAggregator")

  val cmdLineArgs = EventAggregatorArgs()
  val appInstance = new EventAggregator(cmdLineArgs, config, new CachedSchemaRegistryClient(cmdLineArgs.kafkaConfig.schemaRegistryUrl, 100))

  val completionFuture = appInstance.run()

  import scala.concurrent.ExecutionContext.Implicits.global
  completionFuture.andThen {
      case Success(_) =>
        logger.info("Aggregator flow completed exiting app")
        sys.exit(0)

      case Failure(e) =>
        logger.error("Error during flow execution", e)
        sys.exit(-1)
  }

  sys.addShutdownHook {
    logger.info("Shutting down actor system")
    actorSystem.terminate()
    logger.info(".. done")
  }
}
