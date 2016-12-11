package com.pragmasoft.eventaggregator.support

import org.apache.commons.lang3.RandomStringUtils

trait SpecificRecordEventFixture extends RecordEventsSchemas {

  def newEventHeader(id: String, correlationId: Option[String] = None, eventTs: Long = System.currentTimeMillis()): EventHeader = {
    new EventHeader(id, correlationId.orNull, eventTs)
  }

  def randomIdNoCorrelation : EventHeader = newEventHeader(RandomStringUtils.random(10))
}
