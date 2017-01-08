package com.pragmasoft.eventaggregator.streams.esrestwriter

import java.util
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.actor.ActorSubscriber
import akka.testkit.TestKit
import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.EventHeaderDescriptor
import com.pragmasoft.eventaggregator.model.{EventKafkaLocation, KafkaAvroEvent}
import com.pragmasoft.eventaggregator.support.{EventHeader, SpecificRecordEventFixture}
import com.typesafe.scalalogging.LazyLogging
import io.searchbox.action.Action
import io.searchbox.client.{JestClient, JestClientFactory, JestResult, JestResultHandler}
import io.searchbox.core.{DocumentResult, Index}
import org.mockito.Matchers.{anyInt, any => anyArg}
import org.mockito.Mockito.{times, verify, when, atLeast => atLeastTimes}
import org.reactivestreams.Subscription
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

class EsRestActorPoolSubscriberSpec
  extends TestKit(ActorSystem("EsRestActorPoolSubscriberSpec"))
    with WordSpecLike
    with Matchers
    with MockitoSugar
    with Eventually
    with LazyLogging
    with SpecificRecordEventFixture {

  override implicit def patienceConfig = PatienceConfig(timeout = Span(10, Seconds), interval = Span(100, Millis))

  "EsRestActorPoolSubscriber" should {
    "Index a new document in ES when receiving a KafkaAvroEvent" in {
      val jestClientFactory = mock[JestClientFactory]
      val mockJestClient = mock[JestClient]
      when(jestClientFactory.getObject).thenReturn(mockJestClient)

      val esRestActorPoolSubscriber = system.actorOf(
        EsRestActorPoolSubscriber.props(
          numberOfWorkers = 1,
          maxQueueSize = 1,
          elasticSearchIndex = () => "esIndex",
          jestClientFactory = jestClientFactory,
          headerDescriptor = EventHeaderDescriptor(Some("id"), Some("eventTs")),
          writerActorDispatcher = "akka.custom.dispatchers.elasticsearch.writer-dispatcher")
      )

      val actorSubscriber = ActorSubscriber[KafkaAvroEvent[EventHeader]](esRestActorPoolSubscriber)
      actorSubscriber.onNext(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), randomIdNoCorrelation))

      eventually {
        verify(mockJestClient).executeAsync(anyArg[Index], anyArg[JestResultHandler[DocumentResult]])
      }
    }

    "Ask for more content when number of in-flight request is below maxQueueSize" in {
      val mockedSubcription = mock[Subscription]

      val jestClientFactory = mock[JestClientFactory]
      val successfulIndexResult = mock[DocumentResult]
      when(successfulIndexResult.isSucceeded).thenReturn(true)

      val stubJestClient = new JestClient {
        override def shutdownClient(): Unit = ???
        override def execute[T <: JestResult](clientRequest: Action[T]): T = ???
        override def setServers(servers: util.Set[String]): Unit = ???

        import scala.concurrent.ExecutionContext.Implicits.global
        val callCount = new AtomicInteger(0)
        override def executeAsync[T <: JestResult](clientRequest: Action[T], jestResultHandler: JestResultHandler[_ >: T]): Unit = {
          val firstCall = callCount.getAndIncrement() == 0
          Future {
            if (firstCall) {
              logger.info("First call delaying response of 400ms")
              Thread.sleep(400)
            } else {
              logger.info("Subsequent call, delaying reponse of 2s to be sure is outside the test limits")
              Thread.sleep(2000)
            }

            logger.info("Notifying result handler")
            jestResultHandler.completed(successfulIndexResult.asInstanceOf[T])
          }
        }
      }


      when(jestClientFactory.getObject).thenReturn(stubJestClient)


      val esRestActorPoolSubscriber =
        system.actorOf(
          EsRestActorPoolSubscriber.props(
            numberOfWorkers = 1,
            maxQueueSize = 4,
            elasticSearchIndex = () => "esIndex",
            jestClientFactory = jestClientFactory,
            subscriptionRequestBatchSize =  1,
            headerDescriptor = EventHeaderDescriptor(Some("id"), Some("eventTs")),
            writerActorDispatcher = "akka.custom.dispatchers.elasticsearch.writer-dispatcher"
          )
        )

      val actorSubscriber = ActorSubscriber[KafkaAvroEvent[EventHeader]](esRestActorPoolSubscriber)

      actorSubscriber.onSubscribe(mockedSubcription)

      eventually {
        verify(mockedSubcription, times(1)).request(4)
      }

      (1 to 4) foreach { _ => actorSubscriber.onNext(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), randomIdNoCorrelation)) }

      Thread.sleep(200)

      //Still only one request
      verify(mockedSubcription, times(1)).request(anyInt())

      // After the 200 ms of delay
      eventually {
        verify(mockedSubcription, atLeastTimes(1)).request(1)
      }
    }
  }

  "Use a circuit breaker to prevent insisting calling a failing ES instance - number of failures" in {
    val mockedSubcription = mock[Subscription]

    val jestClientFactory = mock[JestClientFactory]

    val jestClientCallCount = new AtomicInteger(0)
    val stubJestClient = new JestClient {
      override def shutdownClient(): Unit = ???
      override def execute[T <: JestResult](clientRequest: Action[T]): T = ???
      override def setServers(servers: util.Set[String]): Unit = ???

      import scala.concurrent.ExecutionContext.Implicits.global

      override def executeAsync[T <: JestResult](clientRequest: Action[T], jestResultHandler: JestResultHandler[_ >: T]): Unit = {
        val firstCall = jestClientCallCount.getAndIncrement() == 0
        Future {
          logger.info("Notifying result handler of failure")
          jestResultHandler.failed(new Exception("Simulating ES failure"))
        }
      }
    }


    when(jestClientFactory.getObject).thenReturn(stubJestClient)

    val esRestActorPoolSubscriber =
      system.actorOf(
        EsRestActorPoolSubscriber.props(
          numberOfWorkers = 1,
          maxQueueSize = 4,
          elasticSearchIndex = () => "esIndex",
          jestClientFactory = jestClientFactory,
          subscriptionRequestBatchSize =  1,
          maxFailures = 1,
          headerDescriptor = EventHeaderDescriptor(Some("id"), Some("eventTs")),
          writerActorDispatcher = "akka.custom.dispatchers.elasticsearch.writer-dispatcher"
        )
      )

    val actorSubscriber = ActorSubscriber[KafkaAvroEvent[EventHeader]](esRestActorPoolSubscriber)

    actorSubscriber.onSubscribe(mockedSubcription)

    //First failure
    actorSubscriber.onNext(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), randomIdNoCorrelation))

    eventually {
      jestClientCallCount.get() shouldEqual 1
    }

    // Waiting for first call to be processed
    eventually {
      verify(mockedSubcription, times(2)).request(anyInt())
    }

    // Other calls
    actorSubscriber.onNext(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), randomIdNoCorrelation))

    //only one request
    Thread.sleep(10)
    jestClientCallCount.get() shouldEqual 1
  }

  "Use a circuit breaker to prevent insisting calling a failing ES instance - timeout" in {
    val mockedSubcription = mock[Subscription]

    val jestClientFactory = mock[JestClientFactory]

    val jestClientCallCount = new AtomicInteger(0)
    val stubJestClient = new JestClient {
      override def shutdownClient(): Unit = ???
      override def execute[T <: JestResult](clientRequest: Action[T]): T = ???
      override def setServers(servers: util.Set[String]): Unit = ???

      import scala.concurrent.ExecutionContext.Implicits.global

      override def executeAsync[T <: JestResult](clientRequest: Action[T], jestResultHandler: JestResultHandler[_ >: T]): Unit = {
        val firstCall = jestClientCallCount.getAndIncrement() == 0
        Future {
          logger.info("Delaying response more than the call timeout")
          Thread.sleep(300)

          val successfulIndexResult = mock[DocumentResult]
          when(successfulIndexResult.isSucceeded).thenReturn(true)

          logger.info("Notifying result handler of late success")
          jestResultHandler.completed(successfulIndexResult.asInstanceOf[T])
        }
      }
    }


    when(jestClientFactory.getObject).thenReturn(stubJestClient)

    val esRestActorPoolSubscriber =
      system.actorOf(
        EsRestActorPoolSubscriber.props(
          numberOfWorkers = 1,
          maxQueueSize = 4,
          elasticSearchIndex = () => "esIndex",
          jestClientFactory = jestClientFactory,
          subscriptionRequestBatchSize =  1,
          maxFailures = 1,
          callTimeout = 200.milliseconds,
          headerDescriptor = EventHeaderDescriptor(Some("id"), Some("eventTs")),
          writerActorDispatcher = "akka.custom.dispatchers.elasticsearch.writer-dispatcher"
        )
      )

    val actorSubscriber = ActorSubscriber[KafkaAvroEvent[EventHeader]](esRestActorPoolSubscriber)

    actorSubscriber.onSubscribe(mockedSubcription)

    //First failure
    actorSubscriber.onNext(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), randomIdNoCorrelation))

    eventually {
      jestClientCallCount.get() shouldEqual 1
    }

    // Waiting for first call to be processed
    Thread.sleep(300)

    // Other calls
    actorSubscriber.onNext(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), randomIdNoCorrelation))

    //only one request
    Thread.sleep(10)
    jestClientCallCount.get() shouldEqual 1
  }

}
