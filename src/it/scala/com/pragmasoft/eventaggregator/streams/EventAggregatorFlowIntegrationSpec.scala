package com.pragmasoft.eventaggregator.streams

import java.util
import java.util.Properties
import java.util.concurrent.TimeoutException
import java.util.regex.Pattern

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import com.pragmasoft.eventaggregator.ActorSystemProvider
import com.pragmasoft.eventaggregator.model.KafkaAvroEvent
import com.pragmasoft.eventaggregator.support.{IntegrationEventsFixture, WithActorSystemIT}
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.KafkaAvroSerializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{Serializer, StringDeserializer}
import org.scalactic.source.Position
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Future

class EventAggregatorFlowIntegrationSpec
  extends WordSpec
    with Matchers
    with EmbeddedKafka
    with LazyLogging
    with WithActorSystemIT
    with Eventually
    with ScalaFutures
    with IntegrationEventsFixture {

  implicit lazy val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  val elasticSearchPatience = PatienceConfig(timeout = Span(10, Seconds), interval = Span(500, Millis))
  val elasticSearchRetrievalPatience = PatienceConfig(timeout = Span(3, Seconds), interval = Span(500, Millis))

  val TestTopic = "testTopic"

  "EventAggregatorFlow" should {
    "Read and parse messages from kafka" in withRunningKafka {
      withActorSystem { implicit _actorSystem  =>
        val event = aProfileCreatedEvent

        val _schemaRegistry = new MockSchemaRegistryClient()
        _schemaRegistry.register(TestTopic, event.getSchema)

        implicit val eventSerializer: Serializer[AnyRef] = new KafkaAvroSerializer(_schemaRegistry)

        val flow = new EventAggregatorFlow[Future[Option[KafkaAvroEvent[GenericRecord]]]] with KafkaSourceProvider with ActorSystemProvider with LazyLogging
          with SinkProvider[KafkaAvroEvent[GenericRecord], Future[Option[KafkaAvroEvent[GenericRecord]]]] {
          override def kafkaConfig = KafkaPublisherConfig(
            reactiveKafkaDispatcher = "akka.custom.dispatchers.kafka-publisher-dispatcher",
            bootstrapBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}",
            topicRegex = TestTopic,
            groupId = "testGroup",
            readFromBeginning = true
          )

          override implicit def actorSystem: ActorSystem = _actorSystem

          override val schemaRegistry: SchemaRegistryClient = _schemaRegistry

          override def sink = Sink.headOption[KafkaAvroEvent[GenericRecord]]
        }

        publishToKafka[AnyRef](TestTopic, event)

        val (_, messageFuture) = flow.startFlow()

        whenReady(messageFuture) { messageMaybe =>
          val message = messageMaybe.get
          message.location.topic shouldBe TestTopic
          message.data.get("firstName") shouldBe event.getFirstName
        }(elasticSearchRetrievalPatience, Position.here)
      }
    }


  }


  private def readNextStreamMessageFromRegex(topicRegex: String): String = {
    val props = new Properties()
    props.put("group.id", s"test")
    props.put("bootstrap.servers", s"localhost:${embeddedKafkaConfig.kafkaPort}")
    props.put("auto.offset.reset", "earliest")

    val consumer =
      new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer)

    val message = try {
      consumer.subscribe(
        Pattern.compile(topicRegex),
        new ConsumerRebalanceListener {
          override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = println("partitions reassigned: " + partitions)

          override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = println("partitions revoked: " + partitions)
        })

      val records = consumer.poll(5000)
      if (records.isEmpty) {
        throw new TimeoutException(
          "Unable to retrieve a message from Kafka in 5000ms")
      }

      records.iterator().next().value()
    } finally {
      consumer.close()
    }

    message
  }
}
