package com.pragmasoft.eventaggregator.model

import org.apache.avro.generic.GenericRecord

case class KafkaAvroEvent[+T <: GenericRecord](location: EventKafkaLocation, data: T) {
  def schemaName: String = data.getSchema.getName
}
