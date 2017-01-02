package com.pragmasoft.eventaggregator

import java.io.ByteArrayOutputStream

import com.pragmasoft.eventaggregator.model.KafkaAvroEvent
import com.sksamuel.elastic4s.source.Indexable
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.joda.time.format.ISODateTimeFormat

object GenericRecordEventJsonConverter {

  case class EventHeaderDescriptor(eventIdPath: Option[String], eventTsPath: Option[String]) {
    import com.pragmasoft.eventaggregator.GenericRecordFieldExtractionSupport._

    def extractEventId[T <: GenericRecord](event: T): Option[String] = eventTsPath.flatMap(event.getField[Any]).map(_.toString)

    def extractEventTs[T <: GenericRecord](event: T): Option[Long] = eventTsPath.flatMap(event.getField[Long])
  }

  private def asJsonString(event: GenericRecord): String = {
    val out = new ByteArrayOutputStream()
    val jsonEncoder = EncoderFactory.get().jsonEncoder(event.getSchema, out)
    val writer = new GenericDatumWriter[GenericRecord](event.getSchema)

    writer.write(event, jsonEncoder)
    jsonEncoder.flush()
    out.close()

    out.toString
  }

  implicit def kafkaAvroEventIndexable(implicit headerDescriptor: EventHeaderDescriptor): Indexable[KafkaAvroEvent[GenericRecord]] = new Indexable[KafkaAvroEvent[GenericRecord]] {
    val timestampFormat = ISODateTimeFormat.dateTime().withZoneUTC

    override def json(event: KafkaAvroEvent[GenericRecord]): String = {
      val timestampJsonAttributeMaybe =
        headerDescriptor.extractEventTs(event.data)
          .map(ts => s""" "@timestamp" : "${timestampFormat.print(ts)}",""")

      s"""{
          |  ${timestampJsonAttributeMaybe.getOrElse("")}
          |  "location" : { "topic" : "${event.location.topic}", "partition" : ${event.location.partition}, "offset" : ${event.location.offset} },
          |  "schemaName" : "${event.schemaName}",
          |  "data" : ${asJsonString(event.data)}
          |} """.stripMargin

    }
  }

}
