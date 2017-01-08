package com.pragmasoft.eventaggregator.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.TestSubscriber.Probe
import akka.stream.testkit.scaladsl.TestSink
import com.pragmasoft.eventaggregator.model.{EventKafkaLocation, KafkaAvroEvent}
import com.pragmasoft.eventaggregator.support.SpecificRecordEventFixture
import com.pragmasoft.eventaggregator.{ActorSystemProvider, WithActorSystem}
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.{KafkaAvroEncoder, KafkaAvroSerializer}
import kafka.serializer.DefaultDecoder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serializer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

import scala.Array.emptyByteArray
import scala.concurrent.Future

trait KafkaEventsFixture {
  def aConsumerRecord(value: Array[Byte], key: Array[Byte] = emptyByteArray, topic: String = "topic", partition: Int = 0, offset: Int = 0): ConsumerRecord[Array[Byte], Array[Byte]] = {
    new ConsumerRecord[Array[Byte], Array[Byte]](
      topic,
      partition,
      offset,
      key,
      value
    )
  }
}

class MonitorPublishingFlowSpec extends WordSpec with Matchers with WithActorSystem with KafkaEventsFixture with ScalaFutures with SpecificRecordEventFixture {

  val schemaRegistry = new MockSchemaRegistryClient()
  val eventEncoder = new KafkaAvroEncoder(schemaRegistry)

  "MonitorPublishingFlow" should {

    "publish a valid event message converting it into a MonitoredEvent" in withActorSystem { actorSystem =>
      val kafkaMessage = aConsumerRecord(value = eventEncoder.toBytes(aProfileCreatedEvent))
      val flow = new StubbedMonitoringFlow(
        Seq(kafkaMessage),
        actorSystem,
        schemaRegistry
      )

      val (_, probe) = flow.startFlow()
      val monitoredEvent = probe.request(1).expectNext()

      monitoredEvent.location should be(EventKafkaLocation.fromKafkaMessage(kafkaMessage))
      monitoredEvent.data.get("userId").toString should be("userId")
    }

    "drop invalid events and progress with valid ones" in withActorSystem { actorSystem =>

      val invalidKafkaMessage = aConsumerRecord(value = "Invalid message".getBytes)

      val validKafkaMessage = aConsumerRecord(value = eventEncoder.toBytes(aProfileCreatedEvent))

      val flow = new StubbedMonitoringFlow(
        Seq(invalidKafkaMessage, validKafkaMessage),
        actorSystem,
        schemaRegistry
      )

      val (_, probe) = flow.startFlow()
      val monitoredEvent = probe.request(1).expectNext()

      monitoredEvent.location should be(EventKafkaLocation.fromKafkaMessage(validKafkaMessage))
      monitoredEvent.data.get("userId").toString should be("userId")
    }


    "Publish all events" in withActorSystem { implicit actorSystem  =>
      val event = aProfileCreatedEvent

      val events = List(aProfileCreatedEvent, aProfileCreatedEvent, aProfileCreatedEvent)

      val flow = aggregatorFlowForEvents(events)

      val (_, messagesFuture) = flow.startFlow()

      whenReady(messagesFuture) { messages =>
        messages.reverse.map(_.data.get("header").asInstanceOf[GenericRecord].get("id").asInstanceOf[String]) shouldBe events.map(_.getHeader.getId)
      }
    }

    "Notify completion" in withActorSystem { implicit actorSystem  =>
      val event = aProfileCreatedEvent

      val events = List(aProfileCreatedEvent)

      val flow = aggregatorFlowForEvents(events)

      val (completedFuture, eventsFuture) = flow.startFlow()

      whenReady(completedFuture) { _ =>
        eventsFuture.isCompleted shouldBe true
      }
    }
  }


  private def aggregatorFlowForEvents[T <: GenericRecord](events: Iterable[T])(implicit _actorSystem: ActorSystem): EventAggregatorFlow[Future[List[KafkaAvroEvent[GenericRecord]]]] = {
    val _schemaRegistry = new MockSchemaRegistryClient()
    events.headOption.foreach { event =>
      _schemaRegistry.register("TestTopic", event.getSchema)
    }

    implicit val eventSerializer: Serializer[AnyRef] = new KafkaAvroSerializer(_schemaRegistry)

    val flow = new EventAggregatorFlow[Future[List[KafkaAvroEvent[GenericRecord]]]] with ActorSystemProvider with LazyLogging
      with SinkProvider[KafkaAvroEvent[GenericRecord], Future[List[KafkaAvroEvent[GenericRecord]]]]
      with SourceProvider[ConsumerRecord[Array[Byte], Array[Byte]], NotUsed] {

      override implicit def actorSystem: ActorSystem = _actorSystem

      override def sink: Sink[KafkaAvroEvent[GenericRecord], Future[List[KafkaAvroEvent[GenericRecord]]]] =
        Sink.fold[List[KafkaAvroEvent[GenericRecord]], KafkaAvroEvent[GenericRecord]](List.empty) { case (accumulator, currEvent) =>
          currEvent :: accumulator
        }

      override def source: Source[ConsumerRecord[Array[Byte], Array[Byte]], NotUsed] =
        Source.fromIterator(() =>
          events.map { event =>
            new ConsumerRecord("TestTopic", 0, 0, emptyByteArray, eventSerializer.serialize("TestTopic", event))
          }.toIterator
        )

      override val schemaRegistry: SchemaRegistryClient = _schemaRegistry
    }

    flow
  }


  lazy val byteDecoder = new DefaultDecoder()

  class StubbedMonitoringFlow(events: Seq[ConsumerRecord[Array[Byte], Array[Byte]]], override val actorSystem: ActorSystem, override val schemaRegistry: SchemaRegistryClient) extends
    EventAggregatorFlow[TestSubscriber.Probe[KafkaAvroEvent[GenericRecord]]]
    with LazyLogging
    with SourceProvider[ConsumerRecord[Array[Byte], Array[Byte]], NotUsed]
    with SinkProvider[KafkaAvroEvent[GenericRecord], TestSubscriber.Probe[KafkaAvroEvent[GenericRecord]]]
    with ActorSystemProvider {

    override def source: Source[ConsumerRecord[Array[Byte], Array[Byte]], NotUsed] = Source.fromIterator(() => events.iterator)

    override def sink: Sink[KafkaAvroEvent[GenericRecord], Probe[KafkaAvroEvent[GenericRecord]]] = TestSink.probe[KafkaAvroEvent[GenericRecord]](actorSystem)
  }

}
