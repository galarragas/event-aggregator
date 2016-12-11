package com.pragmasoft.eventaggregator.support

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

trait RecordEventsSchemas {

  lazy val schemaParser = new Schema.Parser()
  lazy val ProfileCreatedSchema: Schema = ProfileCreated.SCHEMA$

  lazy val EventHeaderSchema: Schema = EventHeader.SCHEMA$

  lazy val NestedHeaderSchema: Schema = schemaParser.parse(
    """
      |{ "type":"record","name":"Header",
      |         "fields":[
      |           {"name":"id","type":{"type":
      |             { "type":"record","name":"SubHeader", "fields":[
      |               {"name":"id","type":{"type":"string","avro.java.string":"String"}},
      |               {"name":"correlationId","type":["null",{"type":"string","avro.java.string":"String"}],"default":null}
      |             ]
      |           },
      |           {"name":"eventTs","type":"long"}
      |          ]
      |       }
    """.stripMargin
  )

  lazy val SubHeaderSchema: Schema = schemaParser.parse(
    """
      |{ "type":"record","name":"SubHeader", "fields":[
      |               {"name":"id","type":{"type":"string","avro.java.string":"String"}},
      |               {"name":"correlationId","type":["null",{"type":"string","avro.java.string":"String"}],"default":null}
      |             ]
      |           }
    """.stripMargin
  )

  lazy val ProfileCreatedWithNestedHeaderSchema: Schema = schemaParser.parse(
    """
      | {
      |   "type":"record",
      |   "name":"ProfileWithNestedHeaderCreated",
      |   "namespace":"com.pragmasoft.eventaggregator",
      |   "fields":[
      |     { "name":"header","type":
      |       { "type":"record","name":"Header",
      |         "fields":[
      |           {"name":"subHeader","type":{"type":
      |             { "type":"record","name":"SubHeader", "fields":[
      |               {"name":"id","type":{"type":"string","avro.java.string":"String"}},
      |               {"name":"correlationId","type":["null",{"type":"string","avro.java.string":"String"}],"default":null}
      |             ]
      |           },
      |           {"name":"eventTs","type":"long"}
      |          ]
      |       }
      |     },
      |     {"name":"userId","type":{"type":"string","avro.java.string":"String"}},
      |     {"name":"username","type":["null",{"type":"string","avro.java.string":"String"}],"default":null},
      |     {"name":"description","type":["null",{"type":"string","avro.java.string":"String"}],"default":null}]
      | }
      |
      """.stripMargin
  )
}

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
    result.put("username", "Stefano")
    result.put("description", "NewUser")

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

