package com.pragmasoft.eventaggregator.support

import org.apache.commons.lang3.RandomStringUtils

trait IntegrationEventsFixture {
  def aProfileCreatedEvent = new ProfileCreated(randomIdNoCorrelation, "userId", "Stefano", "Galarraga", "stefano.galarraga")

  def newEventHeader(id: String, correlationId: Option[String] = None, eventTs: Long = System.currentTimeMillis()): EventHeader = {
    new EventHeader(id, correlationId.orNull, eventTs)
  }

  def randomIdNoCorrelation : EventHeader = newEventHeader(RandomStringUtils.random(10))
}
