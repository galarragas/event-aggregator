package com.pragmasoft.eventaggregator.streams

import java.time.Clock

import org.joda.time.format.DateTimeFormat

trait ElasticSearchIndexNameProvider {
  def elasticSearchIndex: String
}

trait DayPartitionedElasticSearchIndexNameProvider extends ElasticSearchIndexNameProvider {
  def elasticSearchIndexPrefix: String
  def clock: Clock = Clock.systemUTC

  private val dateTimeFormat = DateTimeFormat.forPattern("YYYY.MM.dd")
  override def elasticSearchIndex = {
    val today = dateTimeFormat.print(clock.millis())

    s"$elasticSearchIndexPrefix$today"
  }
}