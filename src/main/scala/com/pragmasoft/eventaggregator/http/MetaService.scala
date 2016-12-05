package com.pragmasoft.eventaggregator.http

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.google.common.io.Resources.getResource
import com.typesafe.scalalogging.LazyLogging

import scala.io.Source
import scala.util.control.NonFatal

class MetaService(stopwatch: Stopwatch, versionResourceName: String = "version.properties") extends LazyLogging {

  private lazy val versionInfo: String =
    try {
      val properties = new Properties()
      properties.load(Source.fromURL(getResource(versionResourceName)).reader())
      properties.getProperty("version")
    } catch {
      case NonFatal(e) =>
        logger.warn("Exception trying to collect version info", e)
        "Unable to retrieve version information"
    }

  def metaInfo = MetaInfo(Uptime(stopwatch.toString, stopwatch.elapsed(TimeUnit.SECONDS)), versionInfo)
}

object MetaService {
  def create() = new MetaService(Stopwatch.createStarted())
  def create(versionResourceName: String) = new MetaService(Stopwatch.createStarted(), versionResourceName)
}

case class Uptime(humanReadable: String, seconds: Long)
case class MetaInfo(uptime: Uptime, version: String)