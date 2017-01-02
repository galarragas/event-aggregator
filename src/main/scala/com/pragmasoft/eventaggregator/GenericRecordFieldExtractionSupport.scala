package com.pragmasoft.eventaggregator

import org.apache.avro.generic.GenericRecord

import scala.annotation.tailrec

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
      val pathNodes = path.split('/').toList

      getNodePathAs(Some(self), pathNodes)
    }

    @tailrec
    private final def getNodePathAs[T](nodeMaybe: Option[GenericRecord], pathNodes: List[String]): Option[T] = {
      (nodeMaybe, pathNodes) match {
        case (None, _) =>
          None

        case (Some(_), Nil) =>
          None

        case (Some(node), head :: Nil) =>
          getNodeChildAs[T](node, head)

        case (Some(node), head :: tail) =>
          getNodePathAs[T](getNodeChildAs[GenericRecord](node, head), tail)
      }
    }

    protected def getNodeChildAs[T](node: GenericRecord, childName: String): Option[T] = {
      for {
      //this is to prevent a nastly NPE when accessing the field as the second line does
        protectFromNPE <- Option(node.getSchema.getField(childName))
        field <- Option(node.get(childName))
      } yield field.asInstanceOf[T]
    }
  }

}
