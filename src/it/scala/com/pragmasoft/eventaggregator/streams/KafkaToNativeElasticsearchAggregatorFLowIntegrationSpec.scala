package com.pragmasoft.eventaggregator.streams

import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.EventHeaderDescriptor
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

  val elasticSearchPatience = PatienceConfig(timeout = Span(10, Seconds), interval = Span(500, Millis))
  val elasticSearchRetrievalPatience = PatienceConfig(timeout = Span(3, Seconds), interval = Span(500, Millis))

  val schemaRegistry = new MockSchemaRegistryClient()

  "KafkaToElasticsearchMonitorPublishingFLow" should {

    "read messages from a kafka topic matching the regex and publish them in ElasticSearch" in withRunningKafka {
      withActorSystem { actorSystem =>

        implicit val eventSerializer: Serializer[AnyRef] = new KafkaAvroSerializer(schemaRegistry)

        val kafkaConfig = KafkaPublisherConfig(
          bootstrapBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}",
          topicRegex = "testTopic",
          groupId = "testGroup",
          readFromBeginning = true
        )

        val flow = new KafkaToNativeElasticsearchAggregatorFLow(
          kafkaConfig,
          EventsIndexPrefix,
          actorSystem,
          schemaRegistry,
          EventHeaderDescriptor(Some("header/id"), Some("event/eventTs")),
          ElasticClient.fromClient(elasticsearchClient)
        )

        flow.startFlow()

        val event = aProfileCreatedEvent
        publishToKafka[AnyRef]("testTopic", event)

        eventually {
          val eventualGetResponse = ElasticClient.fromClient(elasticsearchClient).execute {
            val eventType = event.getSchema().getName

            get id event.getHeader.getId from EventsIndex / eventType
          }

          whenReady(eventualGetResponse) { getResponse =>
            getResponse.isExists should be(true)
          }(elasticSearchRetrievalPatience, Position.here)
        }(elasticSearchPatience, Position.here)
      }
    }

    "read messages from many kafka topics matching the regex and publish them in ElasticSearch" in withRunningKafka {
      withActorSystem { actorSystem =>

        implicit val eventSerializer = new KafkaAvroSerializer(schemaRegistry)

        val kafkaConfig = KafkaPublisherConfig(
          bootstrapBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}",
          topicRegex = "testTopic\\d+",
          groupId = "testGroup",
          readFromBeginning = true
        )

        val flow = new KafkaToNativeElasticsearchAggregatorFLow(
          kafkaConfig,
          EventsIndexPrefix,
          actorSystem,
          schemaRegistry,
          EventHeaderDescriptor(Some("header/id"), Some("event/eventTs")),
          ElasticClient.fromClient(elasticsearchClient)
        )

        flow.startFlow()

        val event1 = aProfileCreatedEvent
        val event2 = aProfileCreatedEvent

        eventually {
          publishToKafka[AnyRef]("testTopic1", event1)
          publishToKafka[AnyRef]("testTopic2", event2)

          val eventualGetResponse1 = new ElasticClient(elasticsearchClient).execute {
            val eventType = event1.getSchema().getName

            get id event1.getHeader.getId from EventsIndex / eventType
          }

          whenReady(eventualGetResponse1) { getResponse =>
            getResponse.isExists should be(true)
          }(elasticSearchRetrievalPatience, Position.here)

          val eventualGetResponse2 = new ElasticClient(elasticsearchClient).execute {
            val eventType = event2.getSchema().getName

            get id event2.getHeader.getId from EventsIndex / eventType
          }

          whenReady(eventualGetResponse2) { getResponse =>
            getResponse.isExists should be(true)
          }(elasticSearchRetrievalPatience, Position.here)
        }(elasticSearchPatience, Position.here)
      }
    }
  }
}
