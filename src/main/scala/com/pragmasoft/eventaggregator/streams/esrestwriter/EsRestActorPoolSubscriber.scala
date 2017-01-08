package com.pragmasoft.eventaggregator.streams.esrestwriter

import akka.actor.{ActorLogging, ActorRef, Props}
import akka.contrib.circuitbreaker.CircuitBreakerProxy.{CircuitBreakerPropsBuilder, CircuitClosed, CircuitHalfOpen, CircuitOpen}
import akka.event.LoggingReceive
import akka.routing.RoundRobinPool
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, MaxInFlightRequestStrategy, RequestStrategy}
import akka.util.Timeout
import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.EventHeaderDescriptor
import com.pragmasoft.eventaggregator.model.KafkaAvroEvent
import com.pragmasoft.eventaggregator.streams.esrestwriter.EsRestActorPoolSubscriber.ShutdownTimeout
import com.pragmasoft.eventaggregator.streams.esrestwriter.EsRestWriterActor.{Write, WriteResult}
import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig
import org.apache.avro.generic.GenericRecord

import scala.concurrent.duration._

object EsRestActorPoolSubscriber {
  object ShutdownTimeout

  def props(
             numberOfWorkers: Int,
             maxQueueSize: Int,
             elasticSearchIndex: () => String,
             jestClientFactory: JestClientFactory,
             headerDescriptor: EventHeaderDescriptor,
             writerActorDispatcher: String,
             maxFailures: Int = 5,
             callTimeout: Timeout = 30.seconds,
             resetTimeout: Timeout = 1.minute,
             subscriptionRequestBatchSize: Int = 5,
             maxShutdownTimeout: FiniteDuration = 2.minutes
           ): Props =
    Props(new EsRestActorPoolSubscriber(
      numberOfWorkers,
      maxQueueSize,
      elasticSearchIndex,
      jestClientFactory,
      writerActorDispatcher,
      maxFailures,
      callTimeout,
      resetTimeout,
      subscriptionRequestBatchSize,
      headerDescriptor, maxShutdownTimeout
    ))

  def props(
             numberOfWorkers: Int,
             maxQueueSize: Int,
             elasticSearchIndex: () => String,
             esConnectionUrl: String,
             headerDescriptor: EventHeaderDescriptor,
             writerActorDispatcher: String
           ): Props = {
    props(numberOfWorkers, maxQueueSize, elasticSearchIndex, jestClientFactory(esConnectionUrl), headerDescriptor, writerActorDispatcher)
  }

  private [EsRestActorPoolSubscriber] def jestClientFactory(esConnectionUrl: String): JestClientFactory = {
    val result = new JestClientFactory()
    result.setHttpClientConfig(
      new HttpClientConfig.Builder(esConnectionUrl)
        .multiThreaded(true)
        .build()
    )
    result
  }
}

class EsRestActorPoolSubscriber(
                                 numberOfWorkers: Int,
                                 maxQueueSize: Int,
                                 elasticSearchIndex: () => String,
                                 jestClientFactory: JestClientFactory,
                                 writerActorDispatcher: String,
                                 maxFailures: Int,
                                 callTimeout: Timeout,
                                 resetTimeout: Timeout,
                                 subscriptionRequestBatchSize: Int,
                                 headerDescriptor: EventHeaderDescriptor,
                                 maxShutdownTimeout: FiniteDuration
                               )  extends ActorSubscriber with ActorLogging {

  log.debug("Initializing EsRestActorPoolSubscriber")

  var inFlightMessages: Set[KafkaAvroEvent[_]] = Set.empty

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy(max = maxQueueSize) {
    override def inFlightInternally: Int = inFlightMessages.size
    override lazy val batchSize = subscriptionRequestBatchSize
  }

  override def receive: Receive = handleWriteProtocol orElse logCircuitEvents

  def shuttingDown: Receive = waitForInFlightMessageCompletion orElse logCircuitEvents

  lazy val esWritersPoolCircuitBreakerProxy : ActorRef = {
    val circuitBreakerBuilder =
      CircuitBreakerPropsBuilder(maxFailures,callTimeout,resetTimeout)
        .copy(
          failureDetector = {
            case writeResult@WriteResult(_, succeeded) =>
              val isFailure = !succeeded
              log.debug("Received result {}, returning failure status {}", writeResult, isFailure)
              isFailure

            case unknown@_ =>
              log.warning("Invalid result {}, considering it a failure", unknown)
              true
          }
        )
        .copy(
          openCircuitFailureConverter = circuitOpenFailure => {
            val failedWriteCommand = circuitOpenFailure.failedMsg.asInstanceOf[Write[GenericRecord]]
            log.info("Converting circuit open failure for command {} to a failed write result", failedWriteCommand)
            WriteResult(failedWriteCommand.event, false)
          }
        )

    val workersPool = context.actorOf(
      RoundRobinPool(numberOfWorkers)
        .props(
          EsRestWriterActor
            .props(jestClientFactory, elasticSearchIndex, headerDescriptor)
            .withDispatcher(writerActorDispatcher)
        ),
      "esWritersPoolCircuitBreakerProxy"
    )

    context.actorOf(
      circuitBreakerBuilder.props(workersPool),
      "serviceCircuitBreaker"
    )
  }

  def handleWriteProtocol : Receive = LoggingReceive {
    case OnNext(event: KafkaAvroEvent[GenericRecord]) =>
      log.debug("New event {}", event)
      assert(inFlightMessages.size < maxQueueSize, s"queued too many: ${inFlightMessages.size}")

      inFlightMessages += event

      log.debug("Number of in-flight events is now {} over a max of {}, asking the workers pool to write it", inFlightMessages.size, maxQueueSize)

      esWritersPoolCircuitBreakerProxy ! Write(event)

    case WriteResult(event, succeeded) =>
      if(succeeded)
        log.info("Event {} written successfully", event)
      else
        log.warning("Failed to write event {} in ES", event)

      inFlightMessages -= event

      log.debug("Number of in-flight events is now {}, number of remaining requested is {}, max in-flight is {}", inFlightMessages.size, remainingRequested, maxQueueSize)

    case OnComplete =>
      if(inFlightMessages.isEmpty) {
        processingCompleted
      } else {
        import context.dispatcher
        context.system.scheduler.scheduleOnce(maxShutdownTimeout, self, ShutdownTimeout)
        context become shuttingDown
      }

    case OnError(e) =>
      log.info("Processing completed with failure {}", e)
      context stop self
  }

  private def waitForInFlightMessageCompletion : Receive = {
    case WriteResult(event, succeeded) =>
      if(succeeded)
        log.info("Event {} written successfully", event)
      else
        log.warning("Failed to write event {} in ES", event)

      inFlightMessages -= event

      if(inFlightMessages.isEmpty) {
        processingCompleted
      }

    case ShutdownTimeout =>
      log.warning(s"Timeout received while shutting down with ${inFlightMessages.size} messages")
      context stop self

  }

  private def processingCompleted = {
    log.info("Processing completed successfully")
    context stop self
  }

  def logCircuitEvents: Receive = {
    case CircuitOpen(_) =>
      log.warning("Circuit breaker is OPEN")

    case CircuitClosed(_) =>
      log.info("Circuit breaker is CLOSED")

    case CircuitHalfOpen(_) =>
      log.info("Circuit breaker is HALF OPEN")
  }
}
