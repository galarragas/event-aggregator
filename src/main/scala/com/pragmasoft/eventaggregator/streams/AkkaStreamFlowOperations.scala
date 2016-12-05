package com.pragmasoft.eventaggregator.streams

import akka.stream.Supervision
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.LazyLogging

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait AkkaStreamFlowOperations {
  self: LazyLogging =>

  protected def alwaysResume(errorMessage: String): Supervision.Decider =  {
    case NonFatal(exc) =>
      logger.error( errorMessage, exc)
      Supervision.Resume
  }

  protected def filterAndLogFailures[T](errorMessage: String) = Flow[Try[T]].map {
    case x @ Failure(ex) =>
      logger.warn(errorMessage, ex)
      identity(x)
    case x =>
      identity(x)
  }.collect { case Success(successfulValue) => successfulValue }

}
