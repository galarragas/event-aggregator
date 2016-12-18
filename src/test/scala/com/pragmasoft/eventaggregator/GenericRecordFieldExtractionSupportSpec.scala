package com.pragmasoft.eventaggregator

import com.pragmasoft.eventaggregator.support.GenericRecordEventFixture
import org.scalatest.{Matchers, OptionValues, WordSpec}

class GenericRecordFieldExtractionSupportSpec extends WordSpec with Matchers with GenericRecordEventFixture with OptionValues {
  import GenericRecordFieldExtractionSupport._

  "getField" should {
    "Extract field of type String from a single nested property" in {
      record.getField[String]("header/correlationId").value shouldBe "CORRELATION_ID"
      record.getField[Long]("header/eventTs").value shouldBe 100l
    }

    "fail for empty path" in {
      intercept[IllegalArgumentException] {
        record.getField[String]("")
      }
    }
  }


}
