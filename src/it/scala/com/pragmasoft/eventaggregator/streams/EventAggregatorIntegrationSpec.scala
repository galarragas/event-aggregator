package com.pragmasoft.eventaggregator.streams

import com.pragmasoft.eventaggregator._
import com.pragmasoft.eventaggregator.support.{ElasticsearchContainer, IntegrationEventsFixture, WithActorSystemIT}
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.Serializer
import org.scalactic.source.Position
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, WordSpec}

class EventAggregatorIntegrationSpec
  extends WordSpec
    with Matchers
    with EmbeddedKafka
    with ElasticsearchContainer
    with LazyLogging
    with WithActorSystemIT
    with Eventually
    with ScalaFutures
    with IntegrationEventsFixture {

  implicit lazy val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  implicit val elasticSearchPatience = PatienceConfig(timeout = Span(20, Seconds), interval = Span(500, Millis))
  val elasticSearchRetrievalPatience = PatienceConfig(timeout = Span(3, Seconds), interval = Span(500, Millis))

  logger.info("Ignore exception in DefaultSchemaRegistry constructor when trying to access consul since it is not configured")
  val schemaRegistry = new MockSchemaRegistryClient()

  implicit val eventSerializer: Serializer[AnyRef] = new KafkaAvroSerializer(schemaRegistry)

  val TestTopic = "testTopic"

  "ConfigurableEventMonitorApp" should {
    "run a Kafka to Native Elasticsearch Event Aggregator Flow using configuration parameters" in withRunningKafka {
      withActorSystem { implicit actorSystem =>

        val config = ConfigFactory.load()
        val app = new EventAggregator(
          EventAggregatorArgs(
            esConfig = EsConfig(esHost = "localhost", esPort = Some(elasticsearch.tcpPort), indexPrefix = EventsIndexPrefix, useHttp = false),
            kafkaConfig = KafkaConfig(
              kafkaBootstrapBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}",
              topicRegex = TestTopic,
              readFromBeginning = true
            ),
            eventsSyntaxConfig = EventsSyntaxConfig(eventIdPath = Some("header/id"), eventTsPath = Some("header/eventTs"))
          ),
          config,
          schemaRegistry,
          Some(elasticsearchClient)
        )

        println(s"Connecting to ElasticSearch at localhost:${elasticsearch.httpPort}/$EventsIndex")

        val event = aProfileCreatedEvent

        publishToKafka[AnyRef](TestTopic, event)

        val completed = app.run()

        completed.onFailure { case failure =>
          logger.error("Flow failed with exception ", failure)
        }

        eventually {
          val eventualGetResponse = new ElasticClient(elasticsearchClient).execute {
            val eventType = event.getSchema.getName

            get id event.getHeader.getId from EventsIndex / eventType
          }

          whenReady(eventualGetResponse) { getResponse =>
            getResponse.isExists should be(true)
          }(elasticSearchRetrievalPatience, Position.here)
        }
      }
    }

    "run a Kafka to HTTP Elasticsearch Event Aggregator Flow using configuration parameters" in withRunningKafka {
      withActorSystem { implicit actorSystem =>

        val config = ConfigFactory.load()
        val app = new EventAggregator(
          EventAggregatorArgs(
            esConfig = EsConfig(esHost = "localhost", esPort = Some(elasticsearch.httpPort), indexPrefix = EventsIndexPrefix, useHttp = true),
            kafkaConfig = KafkaConfig(
              kafkaBootstrapBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}",
              topicRegex = TestTopic,
              readFromBeginning = true
            ),
            eventsSyntaxConfig = EventsSyntaxConfig(eventIdPath = Some("header/id"), eventTsPath = Some("header/eventTs"))
          ),
          config,
          schemaRegistry
        )

        println(s"Connecting to ElasticSearch at localhost:${elasticsearch.httpPort}/$EventsIndex")

        val event = aProfileCreatedEvent

        publishToKafka[AnyRef](TestTopic, event)

        val completed = app.run()

        completed.onFailure { case failure =>
          logger.error("Flow failed with exception ", failure)
        }

        eventually {
          val eventualGetResponse = new ElasticClient(elasticsearchClient).execute {
            val eventType = event.getSchema.getName

            get id event.getHeader.getId from EventsIndex / eventType
          }

          whenReady(eventualGetResponse) { getResponse =>
            getResponse.isExists should be(true)
          }(elasticSearchRetrievalPatience, Position.here)
        }
      }
    }
  }
}
