package com.pragmasoft.eventaggregator

import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.{EventHeaderDescriptor, kafkaAvroEventIndexable}
import com.pragmasoft.eventaggregator.model.{EventKafkaLocation, KafkaAvroEvent}
import com.pragmasoft.eventaggregator.support.{ProfileCreated, SpecificRecordEventFixture}
import org.elasticsearch.common.joda.time.{DateTime, DateTimeZone}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.scalatest.{Matchers, WordSpec}

class GenericRecordEventJsonConverterSpec extends WordSpec with Matchers with SpecificRecordEventFixture {

  implicit val formats = DefaultFormats

  "GenericRecordEventJsonConverter" should {
    "convert a MonitoredEvent to a JSON structure containing the sent message location info " in {

      val jsonStringEvent = kafkaAvroEventIndexable(EventHeaderDescriptor(Some("id"), Some("eventTs"))).json(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), randomIdNoCorrelation))

      val messageLocation = parse(jsonStringEvent) \ "location"

      ( messageLocation \ "topic").extract[String] should be("topic")
      ( messageLocation \ "partition").extract[Int] should be(2)
      ( messageLocation \ "offset").extract[Long] should be(100l)
    }

    "convert a MonitoredEvent to a JSON structure containing the schema name of the event" in {

      val event = randomIdNoCorrelation
      val jsonStringEvent = kafkaAvroEventIndexable(EventHeaderDescriptor(Some("id"), Some("eventTs"))).json(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), event))

      (parse(jsonStringEvent) \ "schemaName").extract[String] should be(event.getSchema.getName)
    }

    "convert a MonitoredEvent with a binary encoded avro event to a JSON structure containing the AVRO Json representation of the event" in {

      val event = newEventHeader("event-id", Some("request-id"), 1000l)
      val jsonStringEvent = kafkaAvroEventIndexable(EventHeaderDescriptor(Some("id"), Some("eventTs"))).json(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), event))

      parse(jsonStringEvent) \ "data" should be(parse("""{ "id" : "event-id", "correlationId": { "string" : "request-id" }, "eventTs": 1000 }"""))
    }

    "convert the 'header.eventTs' field in the message if present into a '@timestamp' field with the ISO representation of the timestamp" in {
      val occurredOn =
        new DateTime(0)
          .withYear(2016).withMonthOfYear(1).withDayOfMonth(15)
            .withHourOfDay(18).withMinuteOfHour(18).withSecondOfMinute(56).withMillisOfSecond(831)
            .withZone(DateTimeZone.UTC)

      val header =  newEventHeader("event-id", Some("request-id"), occurredOn.getMillis)
      val event = new ProfileCreated(header, "userId", "firstName", "lastName", "userName")
      val jsonStringEvent = kafkaAvroEventIndexable(EventHeaderDescriptor(Some("header/id"), Some("header/eventTs"))).json(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), event))

      // Cannot do "2016-01-15T18:18:56.831+00:00"
      (parse(jsonStringEvent) \ "@timestamp").extract[String] should be ("2016-01-15T18:18:56.831Z")
    }

    "NOT write a @timestamp JSON field if the structure has no timestamp property property" in {
      val event = newEventHeader("event-id", Some("request-id"), 1000)
      val jsonStringEvent = kafkaAvroEventIndexable(EventHeaderDescriptor(Some("id"), Some("differentEventTs"))).json(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), event))

      parse(jsonStringEvent) \ "@timestamp" should be (JNothing)
    }
  }

}
