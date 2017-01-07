package com.pragmasoft.eventaggregator.support

import com.pragmasoft.eventaggregator.support.data.{IntegrationEventHeader, IntegrationProfileCreated}
import org.apache.commons.lang3.RandomStringUtils

trait IntegrationEventsFixture {
  def aProfileCreatedEvent = new IntegrationProfileCreated(randomIdNoCorrelation, "userId", "Stefano", "Galarraga", "stefano.galarraga")

  def newEventHeader(id: String, correlationId: Option[String] = None, eventTs: Long = System.currentTimeMillis()): IntegrationEventHeader = {
    new IntegrationEventHeader(id, correlationId.orNull, eventTs)
  }

  def randomIdNoCorrelation : IntegrationEventHeader = newEventHeader(RandomStringUtils.randomAlphanumeric(10))
}
