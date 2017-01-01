package com.pragmasoft.eventaggregator.support

import org.apache.commons.lang3.RandomStringUtils

trait IntegrationEventsFixture {
  def aProfileCreatedEvent = new com.pragmasoft.eventaggregator.support.data.ProfileCreated(randomIdNoCorrelation, "userId", "Stefano", "Galarraga", "stefano.galarraga")

  def newEventHeader(id: String, correlationId: Option[String] = None, eventTs: Long = System.currentTimeMillis()): com.pragmasoft.eventaggregator.support.data.EventHeader = {
    new com.pragmasoft.eventaggregator.support.data.EventHeader(id, correlationId.orNull, eventTs)
  }

  def randomIdNoCorrelation : com.pragmasoft.eventaggregator.support.data.EventHeader = newEventHeader(RandomStringUtils.random(10))
}
