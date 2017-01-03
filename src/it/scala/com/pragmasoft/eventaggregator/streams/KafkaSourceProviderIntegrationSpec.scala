package com.pragmasoft.eventaggregator.streams

import java.util
import java.util.Properties
import java.util.concurrent.TimeoutException
import java.util.regex.Pattern

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import com.pragmasoft.eventaggregator.ActorSystemProvider
import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.EventHeaderDescriptor
import com.pragmasoft.eventaggregator.model.{EventKafkaLocation, KafkaAvroEvent}
import com.pragmasoft.eventaggregator.support.{IntegrationEventsFixture, WithActorSystemIT}
import com.sksamuel.elastic4s.ElasticClient
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.KafkaAvroSerializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serializer, StringDeserializer}
import org.scalactic.source.Position
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Future

class KafkaSourceProviderIntegrationSpec
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

  val schemaRegistry = new MockSchemaRegistryClient()

  val TestTopic = "testTopic"

  "KafkaSourceProvider" should {

    "be supported by embedded kafka when using a pattern subscriber" in withRunningKafka {

      publishStringMessageToKafka(s"${TestTopic}1", "Message")

      readNextStreamMessageFromRegex(s"${TestTopic}.+") shouldBe "Message"
    }

    "work with a very simple flow" in withRunningKafka {
      withActorSystem { implicit actorSystem =>

        implicit val materializer = ActorMaterializer(ActorMaterializerSettings(actorSystem))

        val consumerProperties = {
          ConsumerSettings(actorSystem, new ByteArrayDeserializer, new ByteArrayDeserializer)
            .withBootstrapServers(s"localhost:${embeddedKafkaConfig.kafkaPort}")
            .withGroupId("test")
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        }

        val source: Source[ConsumerRecord[Array[Byte], Array[Byte]], Control] =
          Consumer.atMostOnceSource(consumerProperties, Subscriptions.topicPattern(TestTopic))

        publishStringMessageToKafka(TestTopic, "Message")

        val messageFuture = source.runWith(Sink.headOption)

        whenReady(messageFuture) { message =>
          message.get.value() should be("Message".getBytes)
        }(elasticSearchRetrievalPatience, Position.here)
      }
    }

    "read messages from a kafka topic matching the regex" in withRunningKafka {
      withActorSystem { implicit _actorSystem  =>
        implicit val materializer = ActorMaterializer(ActorMaterializerSettings(_actorSystem))

        implicit val eventSerializer: Serializer[AnyRef] = new KafkaAvroSerializer(schemaRegistry)

        val flow = new KafkaSourceProvider with ActorSystemProvider {
          override def kafkaConfig = KafkaPublisherConfig(
            reactiveKafkaDispatcher = "akka.custom.dispatchers.kafka-publisher-dispatcher",
            bootstrapBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}",
            topicRegex = TestTopic,
            groupId = "testGroup",
            readFromBeginning = true
          )

          override implicit def actorSystem: ActorSystem = _actorSystem
        }

        publishStringMessageToKafka(TestTopic, "Message")

        val messageFuture = flow.source.runWith(Sink.headOption)

        val event = aProfileCreatedEvent
        publishToKafka[AnyRef](TestTopic, event)

        whenReady(messageFuture) { message =>
          message.get.value() should be("Message".getBytes)
        }(elasticSearchRetrievalPatience, Position.here)
      }
    }

    "Read and parse messages in a more complex flow" in withRunningKafka {
      withActorSystem { implicit _actorSystem  =>
        implicit val materializer = ActorMaterializer(ActorMaterializerSettings(_actorSystem))

        implicit val eventSerializer: Serializer[AnyRef] = new KafkaAvroSerializer(schemaRegistry)

        val event = aProfileCreatedEvent

        val _schemaRegistry = new MockSchemaRegistryClient()
        _schemaRegistry.register(TestTopic, event.getSchema)

        val flow = new MonitorPublishingFlow[Future[Option[KafkaAvroEvent[GenericRecord]]]] with KafkaSourceProvider with ActorSystemProvider with LazyLogging
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

        publishStringMessageToKafka(TestTopic, "Message")

        publishToKafka[AnyRef](TestTopic, event)

        val messageFuture = flow.startFlow()

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
