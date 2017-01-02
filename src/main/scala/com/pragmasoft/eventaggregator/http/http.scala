package com.pragmasoft.eventaggregator.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.jackson.Serialization
import org.json4s.{NoTypeHints, jackson}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class HttpServer(port: Int)(implicit actorSystem: ActorSystem) extends LazyLogging with Json4sSupport {

  private val metaService = MetaService.create()

  implicit val materializer = ActorMaterializer()
  implicit val timeout = Timeout(10.seconds)

  implicit val serialization = jackson.Serialization // or native.Serialization
  implicit val jsonFormats = Serialization.formats(NoTypeHints)

  val route =
    path("meta") {
      get {
        complete(metaService.metaInfo)
      }
    }

  def start(): Unit = {
    implicit val executionContext = actorSystem.dispatcher

    Http().bindAndHandle(route, "0.0.0.0", port).onComplete {
      case Success(binding) =>
        logger.info("HTTP server started successfully and bound to {}", binding.localAddress)

      case Failure(ex) =>
        logger.error(s"Error while starting HTTP server ${ex.getMessage}", ex)
    }
  }
}

