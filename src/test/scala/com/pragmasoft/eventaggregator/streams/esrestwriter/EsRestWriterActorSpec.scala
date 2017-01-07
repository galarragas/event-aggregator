package com.pragmasoft.eventaggregator.streams.esrestwriter

import java.util
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.EventHeaderDescriptor
import com.pragmasoft.eventaggregator.model.{EventKafkaLocation, KafkaAvroEvent}
import com.pragmasoft.eventaggregator.support.SpecificRecordEventFixture
import com.typesafe.scalalogging.LazyLogging
import io.searchbox.action.Action
import io.searchbox.client.{JestClient, JestClientFactory, JestResult, JestResultHandler}
import io.searchbox.core.DocumentResult
import org.mockito.Matchers.{any => anyArg, eq => argEq}
import org.mockito.Mockito.when
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Future

class EsRestWriterActorSpec extends TestKit(ActorSystem("EsRestWriterActorSpec"))
  with WordSpecLike with MockitoSugar with Matchers with LazyLogging with SpecificRecordEventFixture with Eventually {


  "EsRestWriterActor" should {

    "Write into ES using the created JEST Client when receiving a Write command" in {

      val jestClientFactory = mock[JestClientFactory]
      val jestClientCallCount = new AtomicInteger(0)
      val stubJestClient = new JestClient {
        override def shutdownClient(): Unit = ???
        override def execute[T <: JestResult](clientRequest: Action[T]): T = ???
        override def setServers(servers: util.Set[String]): Unit = ???
        override def executeAsync[T <: JestResult](clientRequest: Action[T], jestResultHandler: JestResultHandler[_ >: T]): Unit = {
          jestClientCallCount.incrementAndGet()
        }
      }

      when(jestClientFactory.getObject).thenReturn(stubJestClient)

      val writer = system.actorOf(EsRestWriterActor.props(jestClientFactory, () => "Index", EventHeaderDescriptor(Some("header/id"), Some("header/eventTs"))))

      writer ! EsRestWriterActor.Write(KafkaAvroEvent(EventKafkaLocation("topic1", 1, 100), aProfileCreatedEvent))

      eventually {
        jestClientCallCount.get() shouldBe 1
      }
    }


    "Notify the write has been successful with a WriteResult message to the sender" in {

      val jestClientFactory = mock[JestClientFactory]
      val jestClientCallCount = new AtomicInteger(0)
      val stubJestClient = new JestClient {
        override def shutdownClient(): Unit = ???
        override def execute[T <: JestResult](clientRequest: Action[T]): T = ???
        override def setServers(servers: util.Set[String]): Unit = ???

        import scala.concurrent.ExecutionContext.Implicits.global

        override def executeAsync[T <: JestResult](clientRequest: Action[T], jestResultHandler: JestResultHandler[_ >: T]): Unit = {
          jestClientCallCount.incrementAndGet()
          Future {
            val successfulIndexResult = mock[DocumentResult]
            when(successfulIndexResult.isSucceeded).thenReturn(true)
            jestResultHandler.completed(successfulIndexResult.asInstanceOf[T])
          }
        }
      }

      when(jestClientFactory.getObject).thenReturn(stubJestClient)

      val writer = system.actorOf(EsRestWriterActor.props(jestClientFactory, () => "Index", EventHeaderDescriptor(Some("header/id"), Some("header/eventTs"))))

      val sender = TestProbe()
      val event = KafkaAvroEvent(EventKafkaLocation("topic1", 1, 100), aProfileCreatedEvent)
      sender.send(writer, EsRestWriterActor.Write(event))

      sender.expectMsg(EsRestWriterActor.WriteResult(event, true))
    }

    "Notify the write failed with a WriteResult message to the sender" in {

      val jestClientFactory = mock[JestClientFactory]
      val jestClientCallCount = new AtomicInteger(0)
      val stubJestClient = new JestClient {
        override def shutdownClient(): Unit = ???
        override def execute[T <: JestResult](clientRequest: Action[T]): T = ???
        override def setServers(servers: util.Set[String]): Unit = ???

        import scala.concurrent.ExecutionContext.Implicits.global

        override def executeAsync[T <: JestResult](clientRequest: Action[T], jestResultHandler: JestResultHandler[_ >: T]): Unit = {
          jestClientCallCount.incrementAndGet()
          Future {
            val successfulIndexResult = mock[DocumentResult]
            when(successfulIndexResult.isSucceeded).thenReturn(false)
            jestResultHandler.completed(successfulIndexResult.asInstanceOf[T])
          }
        }
      }

      when(jestClientFactory.getObject).thenReturn(stubJestClient)

      val writer = system.actorOf(EsRestWriterActor.props(jestClientFactory, () => "Index", EventHeaderDescriptor(Some("header/id"), Some("header/eventTs"))))

      val sender = TestProbe()
      val event = KafkaAvroEvent(EventKafkaLocation("topic1", 1, 100), aProfileCreatedEvent)
      sender.send(writer, EsRestWriterActor.Write(event))

      sender.expectMsg(EsRestWriterActor.WriteResult(event, false))
    }

    "Notify the write failed with an exception with a WriteResult message to the sender" in {

      val jestClientFactory = mock[JestClientFactory]
      val jestClientCallCount = new AtomicInteger(0)
      val stubJestClient = new JestClient {
        override def shutdownClient(): Unit = ???
        override def execute[T <: JestResult](clientRequest: Action[T]): T = ???
        override def setServers(servers: util.Set[String]): Unit = ???

        import scala.concurrent.ExecutionContext.Implicits.global

        override def executeAsync[T <: JestResult](clientRequest: Action[T], jestResultHandler: JestResultHandler[_ >: T]): Unit = {
          jestClientCallCount.incrementAndGet()
          Future {
            jestResultHandler.failed(new Exception("Simulating an exception while writing to ES"))
          }
        }
      }

      when(jestClientFactory.getObject).thenReturn(stubJestClient)

      val writer = system.actorOf(EsRestWriterActor.props(jestClientFactory, () => "Index", EventHeaderDescriptor(Some("header/id"), Some("header/eventTs"))))

      val sender = TestProbe()
      val event = KafkaAvroEvent(EventKafkaLocation("topic1", 1, 100), aProfileCreatedEvent)
      sender.send(writer, EsRestWriterActor.Write(event))

      sender.expectMsg(EsRestWriterActor.WriteResult(event, false))
    }
  }



}
