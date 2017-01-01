package com.pragmasoft.eventaggregator.streams

import com.pragmasoft.eventaggregator.support.{ElasticsearchContainer, IntegrationEventsFixture, WithActorSystemIT}
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.ProducerRecord
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

  "ConfigurableEventMonitorApp" should {
    "run a KafkaToElasticsearchMonitorPublishingFlow using configuration parameters" in withRunningKafka {

      val app = new ConfigurableEventMonitorApp(
        ConfigFactory.parseString(
          s"""
             | elasticsearch {
             |  host = "localhost"
             |  port = "${elasticsearch.httpPort}"
             |
             |  indexPrefix = $EventsIndexPrefix
             |}
             |
             |http.port = 19999
             |
             |kafka {
             |  broker_list = localhost":${embeddedKafkaConfig.kafkaPort}"
             |  zookeeper_host = localhost":${embeddedKafkaConfig.zooKeeperPort}"
             |
             |  topics_regex = ".+"
             |  consumer_group = "kafka-event-monitor"
             |  actor_dispatcher_name = "kafka-publisher-dispatcher"
             |  read_from_beginning = true
             |}
           """.stripMargin
        ),
        schemaRegistry
      )

      println(s"Connecting to ElasticSearch at localhost:${elasticsearch.httpPort}/$EventsIndex")

      app.run()

      val producer = aKafkaProducer[AnyRef]

      val event = aProfileCreatedEvent
      producer.send(new ProducerRecord("testTopic", event))

      eventually {
        val eventualGetResponse = new ElasticClient(elasticsearchClient).execute {
          val eventType = event.getSchema().getName

          get id event.getHeader.getId from EventsIndex / eventType
        }

        whenReady(eventualGetResponse) { getResponse =>
          getResponse.isExists should be(true)
        }(elasticSearchRetrievalPatience, Position.here)
      }
    }
  }

}
