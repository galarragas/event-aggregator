package com.pragmasoft.eventaggregator.streams

import com.pragmasoft.eventaggregator.support.{ElasticsearchContainer, EmbeddedKafkaExtension, WithActorSystemIT}
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.typesafe.scalalogging.LazyLogging
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, WordSpec}

class KafkaToElasticsearchMonitorPublishingFLowIntegrationSpec
  extends WordSpec
    with Matchers
    with EmbeddedKafkaExtension
    with ElasticsearchContainer
    with LazyLogging
    with WithActorSystemIT
    with Eventually
    with ScalaFutures {

  implicit lazy val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  implicit val elasticSearchPatience = PatienceConfig(timeout = Span(10, Seconds), interval = Span(500, Millis))
  val elasticSearchRetrievalPatience = PatienceConfig(timeout = Span(3, Seconds), interval = Span(500, Millis))

  val schemaRegistry = SchemaRegistry.fromExistingVersions(Seq(AvroSchemaVersion.forSchema(TrackListened.SCHEMA$)))

  def aTrackListenedEvent = TrackListened(newEventHeader, "userId", true, 1, 1001l, SourceTrackId("source", "trackId"), TrackMetadata())

  "KafkaToElasticsearchMonitorPublishingFLow" should {

    "read messages from a kafka topic matching the regex and publish them in ElasticSearch" in withRunningKafka {
      withActorSystem { actorSystem =>

        implicit val eventSerializer = AvroSpecificRecordSerializer[TrackListened](schemaRegistry = schemaRegistry)

        val kafkaConfig = KafkaPublisherConfig(
          kafkaBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}",
          zooKeeperHost = s"localhost:${embeddedKafkaConfig.zooKeeperPort}",
          topicRegex = "testTopic",
          groupId = "testGroup",
          readFromBeginning = true
        )

        val flow = new KafkaToElasticsearchMonitorPublishingFLow(kafkaConfig, EventsIndexPrefix, elasticSearchConnectionUrl, actorSystem, schemaRegistry)

        flow.startFlow()

        val event = aTrackListenedEvent
        publishEvent("testTopic", event)

        eventually {
          val eventualGetResponse = new ElasticClient(elasticsearchClient).execute {
            val eventType = event.getSchema().getName

            get id event.header.eventId from EventsIndex / eventType
          }

          whenReady(eventualGetResponse) { getResponse =>
            getResponse.isExists should be(true)
          }(elasticSearchRetrievalPatience)
        }
      }
    }

    "read messages from many kafka topics matching the regex and publish them in ElasticSearch" in withRunningKafka {
      withActorSystem { actorSystem =>

        implicit val eventSerializer = AvroSpecificRecordSerializer[TrackListened](schemaRegistry = schemaRegistry)

        val kafkaConfig = KafkaPublisherConfig(
          kafkaBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}",
          zooKeeperHost = s"localhost:${embeddedKafkaConfig.zooKeeperPort}",
          topicRegex = "testTopic\\d+",
          groupId = "testGroup",
          readFromBeginning = true
        )

        val flow = new KafkaToElasticsearchMonitorPublishingFLow(kafkaConfig, EventsIndexPrefix, elasticSearchConnectionUrl, actorSystem, schemaRegistry)

        flow.startFlow()

        val event1 = aTrackListenedEvent
        val event2 = aTrackListenedEvent

        eventually {
          publishEvent("testTopic1", event1)
          publishEvent("testTopic2", event2)

          val eventualGetResponse1 = new ElasticClient(elasticsearchClient).execute {
            val eventType = event1.getSchema().getName

            get id event1.header.eventId from EventsIndex / eventType
          }

          whenReady(eventualGetResponse1) { getResponse =>
            getResponse.isExists should be(true)
          }(elasticSearchRetrievalPatience)

          val eventualGetResponse2 = new ElasticClient(elasticsearchClient).execute {
            val eventType = event2.getSchema().getName

            get id event2.header.eventId from EventsIndex / eventType
          }

          whenReady(eventualGetResponse2) { getResponse =>
            getResponse.isExists should be(true)
          }(elasticSearchRetrievalPatience)
        }
      }
    }
  }
}
