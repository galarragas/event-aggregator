package com.pragmasoft.eventaggregator.support

import org.apache.avro.Schema


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
