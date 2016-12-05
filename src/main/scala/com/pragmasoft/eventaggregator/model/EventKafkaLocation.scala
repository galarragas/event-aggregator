package com.pragmasoft.eventaggregator.model

import org.apache.kafka.clients.consumer.ConsumerRecord

case class EventKafkaLocation(topic: String, partition: Int, offset: Long)

object EventKafkaLocation {
  def fromKafkaMessage(message: ConsumerRecord[_, _]) = EventKafkaLocation(message.topic, message.partition, message.offset)
}

