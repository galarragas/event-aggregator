package com.pragmasoft.eventaggregator.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.TestSubscriber.Probe
import akka.stream.testkit.scaladsl.TestSink
import com.pragmasoft.eventaggregator.model.{EventKafkaLocation, KafkaAvroEvent}
import com.pragmasoft.eventaggregator.support.{ProfileCreated, SpecificRecordEventFixture}
import com.pragmasoft.eventaggregator.{ActorSystemProvider, WithActorSystem}
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.KafkaAvroEncoder
import kafka.serializer.DefaultDecoder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest.{Matchers, WordSpec}

import scala.Array.emptyByteArray

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

class MonitorPublishingFlowSpec extends WordSpec with Matchers with WithActorSystem with KafkaEventsFixture with SpecificRecordEventFixture {

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

      val monitoredEvent = flow.startFlow().request(1).expectNext()

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

      val monitoredEvent = flow.startFlow().request(2).expectNext()

      monitoredEvent.location should be(EventKafkaLocation.fromKafkaMessage(validKafkaMessage))
      monitoredEvent.data.get("userId").toString should be("userId")
    }

  }

  lazy val byteDecoder = new DefaultDecoder()

  class StubbedMonitoringFlow(events: Seq[ConsumerRecord[Array[Byte], Array[Byte]]], override val actorSystem: ActorSystem, override val schemaRegistry: SchemaRegistryClient) extends
    MonitorPublishingFlow[TestSubscriber.Probe[KafkaAvroEvent[GenericRecord]]]
    with LazyLogging
    with SourceProvider[ConsumerRecord[Array[Byte], Array[Byte]], NotUsed]
    with SinkProvider[KafkaAvroEvent[GenericRecord], TestSubscriber.Probe[KafkaAvroEvent[GenericRecord]]]
    with ActorSystemProvider {

    override def source: Source[ConsumerRecord[Array[Byte], Array[Byte]], NotUsed] = Source.fromIterator(() => events.iterator)

    override def sink: Sink[KafkaAvroEvent[GenericRecord], Probe[KafkaAvroEvent[GenericRecord]]] = TestSink.probe[KafkaAvroEvent[GenericRecord]](actorSystem)
  }

}
