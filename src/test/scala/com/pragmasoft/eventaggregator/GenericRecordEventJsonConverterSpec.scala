package com.pragmasoft.eventaggregator

import java.lang.System.currentTimeMillis

import com.pragmasoft.eventaggregator.GenericRecordEventJsonConverter.kafkaAvroEventIndexable
import com.pragmasoft.eventaggregator.model.{EventKafkaLocation, KafkaAvroEvent}
import org.apache.commons.lang3.RandomStringUtils
import org.elasticsearch.common.joda.time.{DateTime, DateTimeZone}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.scalatest.{Matchers, WordSpec}
import SimpleEvent.newEvent

case class SimpleEvent(eventId: String, requestId: Option[String], occurredOn: Long)

object SimpleEvent {
  def newEvent : SimpleEvent = SimpleEvent(RandomStringUtils.random(10), None, currentTimeMillis())
}

case class ComplexEvent
class GenericRecordEventJsonConverterSpec extends WordSpec with Matchers {

  implicit val formats = DefaultFormats

  "GenericRecordEventJsonConverter" should {
    "convert a MonitoredEvent to a JSON structure containing the sent message location info " in {

      val jsonStringEvent = kafkaAvroEventIndexable.json(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), newEventHeader))

      val messageLocation = (parse(jsonStringEvent) \ "location")

      ( messageLocation \ "topic").extract[String] should be("topic")
      ( messageLocation \ "partition").extract[Int] should be(2)
      ( messageLocation \ "offset").extract[Long] should be(100l)
    }

    "convert a MonitoredEvent to a JSON structure containing the schema name of the event" in {

      val event = newEvent
      val jsonStringEvent = kafkaAvroEventIndexable.json(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), event))

      (parse(jsonStringEvent) \ "schemaName").extract[String] should be(event.getSchema().getName)
    }

    "convert a MonitoredEvent with a binary encoded avro event to a JSON structure containing the AVRO Json representation of the event" in {

      val event = SimpleEvent("event-id", Some("request-id"), 1000l)
      val jsonStringEvent = kafkaAvroEventIndexable.json(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), event))

      parse(jsonStringEvent) \ "data" should be(parse("""{ "eventId" : "event-id", "requestId": { "string" : "request-id" }, "occurredOn": 1000 }"""))
    }

    "convert the 'header.occurredOn' field in the message if present into a '@timestamp' field with the ISO representation of the timestamp" in {
      val occurredOn =
        (new DateTime(0))
          .withYear(2016).withMonthOfYear(1).withDayOfMonth(15)
            .withHourOfDay(18).withMinuteOfHour(18).withSecondOfMinute(56).withMillisOfSecond(831)
            .withZone(DateTimeZone.UTC)

      val header =  SimpleEvent("event-id", Some("request-id"), occurredOn.getMillis)
      val event = CrowdCreated(header, "userId", "crowdId", "avatarUrl", "crowdName", Some(true))
      val jsonStringEvent = kafkaAvroEventIndexable.json(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), event))

      // Cannot do "2016-01-15T18:18:56.831+00:00"
      (parse(jsonStringEvent) \ "@timestamp").extract[String] should be ("2016-01-15T18:18:56.831Z")
    }

    "NOT write a @timestamp JSON field if the structure has no 'header.occurredOn' property" in {
      val event = EventHeader("event-id", Some("request-id"), 1000)
      val jsonStringEvent = kafkaAvroEventIndexable.json(KafkaAvroEvent(EventKafkaLocation("topic", 2, 100l), event))

      parse(jsonStringEvent) \ "@timestamp" should be (JNothing)
    }
  }

}
