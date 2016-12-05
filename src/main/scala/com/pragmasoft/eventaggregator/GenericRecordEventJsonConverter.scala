package com.pragmasoft.eventaggregator

import java.io.ByteArrayOutputStream

import com.pragmasoft.eventaggregator.model.KafkaAvroEvent
import com.sksamuel.elastic4s.source.Indexable
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat

object GenericRecordEventJsonConverter {

  private def asJsonString(event: GenericRecord): String = {
    val out = new ByteArrayOutputStream()
    val jsonEncoder = EncoderFactory.get().jsonEncoder(event.getSchema, out)
    val writer = new GenericDatumWriter[GenericRecord](event.getSchema)

    writer.write(event, jsonEncoder)
    jsonEncoder.flush()
    out.close()

    out.toString
  }

  implicit object kafkaAvroEventIndexable extends Indexable[KafkaAvroEvent[GenericRecord]] {
    val timestampFormat = ISODateTimeFormat.dateTime().withZoneUTC

    override def json(event: KafkaAvroEvent[GenericRecord]): String = {
      val timestampJsonAttributeMaybe = for {
        field <- Option(event.data.getSchema.getField("header"))  //this is to prevent a nastly NPE when accessing the field as the second line does
        header <- Option(event.data.get("header")).map(_.asInstanceOf[GenericRecord])
        ts <- Option(header.get("occurredOn")).map(_.asInstanceOf[Long])
      } yield s""" "@timestamp" : "${timestampFormat.print(ts)}","""

      s"""{
          |  ${timestampJsonAttributeMaybe.getOrElse("")}
          |  "location" : { "topic" : "${event.location.topic}", "partition" : ${event.location.partition}, "offset" : ${event.location.offset} },
          |  "schemaName" : "${event.schemaName}",
          |  "data" : ${asJsonString(event.data)}
          |} """.stripMargin

    }
  }

}
