package com.pragmasoft.eventaggregator.streams

import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.EventHeaderDescriptor
import com.pragmasoft.eventaggregator.support.data.IntegrationProfileCreated
import com.pragmasoft.eventaggregator.support.{ElasticsearchContainer, IntegrationEventsFixture, WithActorSystemIT}
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.Serializer
import org.scalactic.source.Position
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, WordSpec}

class KafkaToNativeElasticsearchAggregatorFLowIntegrationSpec
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

  val elasticSearchPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(500, Millis))
  val elasticSearchRetrievalPatience = PatienceConfig(timeout = Span(3, Seconds), interval = Span(500, Millis))

  val TestTopic = "testTopic"

  "KafkaToNativeElasticsearchAggregatorFLow" should {
    "read messages from a kafka topic matching the regex and publish them in ElasticSearch" in withRunningKafka {
      withActorSystem { actorSystem =>

        val event = aProfileCreatedEvent

        val schemaRegistry = new MockSchemaRegistryClient()
        schemaRegistry.register(TestTopic, event.getSchema)

        implicit val eventSerializer: Serializer[AnyRef] = new KafkaAvroSerializer(schemaRegistry)

        val kafkaConfig = KafkaPublisherConfig(
          reactiveKafkaDispatcher = "akka.custom.dispatchers.kafka-publisher-dispatcher",
          bootstrapBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}",
          topicRegex = TestTopic,
          groupId = "testGroup",
          readFromBeginning = true
        )

        val flow = new KafkaToNativeElasticsearchAggregatorFLow(
          kafkaConfig,
          EventsIndexPrefix,
          actorSystem,
          schemaRegistry,
          EventHeaderDescriptor(Some("header/id"), Some("header/eventTs")),
          ElasticClient.fromClient(elasticsearchClient)
        )

        flow.startFlow()

        publishToKafka[AnyRef](TestTopic, event)

        eventually {
          logger.info("Looking for an indexed document in ES")
          val eventualGetResponse = ElasticClient.fromClient(elasticsearchClient).execute {
            val eventType = event.getSchema.getName

            get id event.getHeader.getId from EventsIndex / eventType
          }

          whenReady(eventualGetResponse) { getResponse =>
            withClue(s"Expected a document in elasticsearch $EventsIndex/${event.getSchema.getName} with ID ${event.getHeader.getId}") {
              getResponse.isExists should be(true)
            }
          }(elasticSearchRetrievalPatience, Position.here)
        }(elasticSearchPatience, Position.here)
      }
    }

    "read messages from many kafka topics matching the regex and publish them in ElasticSearch" ignore withRunningKafka {
      withActorSystem { actorSystem =>

        val TestTopic1 = s"${TestTopic}1"
        val TestTopic2 = s"${TestTopic}2"

        val schemaRegistry = new MockSchemaRegistryClient()
        schemaRegistry.register(TestTopic1, IntegrationProfileCreated.SCHEMA$)
        schemaRegistry.register(TestTopic2, IntegrationProfileCreated.SCHEMA$)

        implicit val eventSerializer = new KafkaAvroSerializer(schemaRegistry)

        val kafkaConfig = KafkaPublisherConfig(
          reactiveKafkaDispatcher = "akka.custom.dispatchers.kafka-publisher-dispatcher",
          bootstrapBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}",
          topicRegex = s"$TestTopic\\d+",
          groupId = "testGroup",
          readFromBeginning = true
        )

        val flow = new KafkaToNativeElasticsearchAggregatorFLow(
          kafkaConfig,
          EventsIndexPrefix,
          actorSystem,
          schemaRegistry,
          EventHeaderDescriptor(Some("header/id"), Some("header/eventTs")),
          ElasticClient.fromClient(elasticsearchClient)
        )

        flow.startFlow()

        val event1 = aProfileCreatedEvent
        val event2 = aProfileCreatedEvent

        eventually {
          publishToKafka[AnyRef](TestTopic1, event1)
          publishToKafka[AnyRef](TestTopic2, event2)

          val eventualGetResponse1 = new ElasticClient(elasticsearchClient).execute {
            val eventType = event1.getSchema.getName

            get id event1.getHeader.getId from EventsIndex / eventType
          }

          whenReady(eventualGetResponse1) { getResponse =>
            getResponse.isExists should be(true)
          }(elasticSearchRetrievalPatience, Position.here)

          val eventualGetResponse2 = new ElasticClient(elasticsearchClient).execute {
            val eventType = event2.getSchema.getName

            get id event2.getHeader.getId from EventsIndex / eventType
          }

          whenReady(eventualGetResponse2) { getResponse =>
            withClue("Expected a document in elasticsearch") {
              getResponse.isExists should be(true)
            }
          }(elasticSearchRetrievalPatience, Position.here)
        }(elasticSearchPatience, Position.here)
      }
    }
  }
}
