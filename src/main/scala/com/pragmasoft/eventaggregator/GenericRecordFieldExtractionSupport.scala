package com.pragmasoft.eventaggregator

import org.apache.avro.generic.GenericRecord

object GenericRecordFieldExtractionSupport {

  implicit class PimpedGenericRecord(val self: GenericRecord) extends AnyVal {
    /**
      * Extracts the field of type T located path `path` where any nested property is seperated by /
      * Examples header/id or body/property1/subproperty2
      *
      * @return
      */
    def getField[T](path: String): Option[T] = {
      require(path.trim.nonEmpty, "Invalid empty path")

      val pathNodes = path.split('/')

      for {
      //this is to prevent a nastly NPE when accessing the field as the second line does
        protectFromNPE <- Option(self.getSchema.getField(pathNodes(0)))

        fieldContainerPath = pathNodes.take(pathNodes.length - 1)

        fieldContainer <- fieldContainerPath.foldLeft(Option(self)) { case (currRecord, propertyName) =>
          currRecord.flatMap(record => Option(record.get(propertyName))).map(_.asInstanceOf[GenericRecord])
        }

        field <- Option(fieldContainer.get(pathNodes.last))
      } yield field.asInstanceOf[T]
    }
  }

}
