package com.pragmasoft.eventaggregator

import com.pragmasoft.eventaggregator.support.GenericRecordEventFixture
import org.scalatest.{Matchers, OptionValues, WordSpec}

class GenericRecordFieldExtractionSupportSpec extends WordSpec with Matchers with GenericRecordEventFixture with OptionValues {
  import GenericRecordFieldExtractionSupport._

  "getField" should {
    "Extract field of type String from a single nested property" in {
      record.getField[String]("header/correlationId").value shouldBe "CORRELATION_ID"
    }

    "Extract field of type Long from a single nested property" in {
      record.getField[Long]("header/eventTs").value shouldBe 100l
    }

    "Return None for non existent path" in {
      record.getField[Long]("header/eventTs-wrong") shouldBe None
    }

    "fail for empty path" in {
      intercept[IllegalArgumentException] {
        record.getField[String]("")
      }
    }
  }


}
