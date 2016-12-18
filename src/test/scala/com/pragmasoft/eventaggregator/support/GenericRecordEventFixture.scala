package com.pragmasoft.eventaggregator.support

import org.apache.avro.generic.{GenericData, GenericRecord}

trait GenericRecordEventFixture extends RecordEventsSchemas {
  def newEventHeader : GenericRecord = newEventHeader("ID", None, System.currentTimeMillis())

  def newEventHeader(id: String, correlationId: Option[String], eventTs: Long) : GenericRecord = {
    val header = new GenericData.Record(EventHeaderSchema)
    header.put("id", id)
    header.put("correlationId", correlationId.orNull)
    header.put("eventTs", eventTs)

    header
  }

  lazy val record: GenericRecord = {
    val result = new GenericData.Record(ProfileCreatedSchema)

    result.put("header", newEventHeader("RECORD_ID", Some("CORRELATION_ID"), 100l))

    result.put("userId", "UserID")
    result.put("firstName", "Stefano")
    result.put("lastName", "Galarraga")
    result.put("username", "stefano.galarraga")

    result
  }

  lazy val recordWithMoreNestedHeader : GenericRecord = {
    val subHeader = new GenericData.Record(SubHeaderSchema)
    subHeader.put("id", "RECORD_ID")
    subHeader.put("correlationId", "CORRELATION_ID")

    val header = new GenericData.Record(NestedHeaderSchema)
    header.put("subHeader", subHeader)
    header.put("eventTs", 100l)

    val result = new GenericData.Record(ProfileCreatedSchema)
    result.put("header", header)

    result.put("userId", "UserID")
    result.put("username", "Stefano")
    result.put("description", "NewUser")

    result
  }
}

