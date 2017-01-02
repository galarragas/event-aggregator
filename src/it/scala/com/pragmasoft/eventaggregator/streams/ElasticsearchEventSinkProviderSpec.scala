package com.pragmasoft.eventaggregator.streams

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import com.pragmasoft.eventaggregator.ActorSystemProvider
import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.EventHeaderDescriptor
import com.pragmasoft.eventaggregator.model.{EventKafkaLocation, KafkaAvroEvent}
import com.pragmasoft.eventaggregator.support.{ElasticsearchContainer, IntegrationEventsFixture, WithActorSystemIT}
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.generic.GenericRecord
import org.scalactic.source.Position
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, WordSpec}

class ElasticsearchEventSinkProviderSpec
  extends WordSpec
    with Matchers
    with ElasticsearchContainer
    with Eventually
    with ScalaFutures
    with WithActorSystemIT
    with LazyLogging
    with IntegrationEventsFixture {

  val elasticSearchPatience = PatienceConfig(timeout = Span(20, Seconds), interval = Span(500, Millis))
  val elasticSearchRetrievalPatience = PatienceConfig(timeout = Span(3, Seconds), interval = Span(500, Millis))

  "ElasticsearchEventSink" should {
    "Write events in elastic search with document type equal to the schema name using the event ID from header if available" in withActorSystem { actorSystem =>
      val event = aProfileCreatedEvent
      val testFlow = new TestFlow(
        Seq(KafkaAvroEvent(EventKafkaLocation("topic", 1, 100l), event)),
        actorSystem,
        EventHeaderDescriptor(Some("header/id"), Some("header/eventTs"))
      )

      testFlow.runFlow()

      eventually {
        val eventualGetResponse = new ElasticClient(elasticsearchClient).execute {
          val eventType = event.getSchema().getName

          get id event.getHeader.getId from EventsIndex / eventType
        }

        whenReady(eventualGetResponse) { getResponse =>
          getResponse.isExists shouldBe true
        }(elasticSearchRetrievalPatience, Position.here)
      }(elasticSearchPatience, Position.here)
    }

    "Write events in elastic search with document type equal to the schema name using a generated ID if the event has no field 'header' of type EventHeader" in withActorSystem { actorSystem =>
      val event = randomIdNoCorrelation
      val testFlow = new TestFlow(
        Seq(KafkaAvroEvent(EventKafkaLocation("topic", 1, 100l), event)),
        actorSystem,
        EventHeaderDescriptor(Some("header/id"), Some("header/eventTs"))
      )

      testFlow.runFlow()

      eventually {
        val eventualGetResponse = new ElasticClient(elasticsearchClient).execute {
          val eventType = event.getSchema().getName

          search in EventsIndex / eventType query matchQuery("data.eventId", event.getId)
        }

        whenReady(eventualGetResponse) { getResponse =>
          getResponse.getHits.totalHits() shouldBe 1
        }(elasticSearchRetrievalPatience, Position.here)
      }(elasticSearchPatience, Position.here)
    }

  }


  class TestFlow(events: Seq[KafkaAvroEvent[GenericRecord]], override val actorSystem: ActorSystem, override val headerDescriptor: EventHeaderDescriptor)
    extends ElasticsearchEventSinkProvider
      with ElasticSearchIndexNameProvider
      with ActorSystemProvider
      with AkkaStreamFlowOperations
      with LazyLogging {

    override def elasticSearchIndex: String = EventsIndex

    override def elasticSearchClient: ElasticClient = new ElasticClient(elasticsearchClient)

    lazy val flow =
      Source.fromIterator(() => events.iterator)
        .map { event =>
          logger.info("Processing event {}", event)
          event
        }
        .to(sink)

    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(actorSystem))(actorSystem)

    def runFlow(): Unit = {
      flow.run()
    }
  }

}
