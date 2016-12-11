package com.pragmasoft.eventaggregator

import com.pragmasoft.eventaggregator.support.GenericRecordEventFixture
import org.scalatest.{Matchers, WordSpec}

class GenericRecordFieldExtractionSupportSpec extends WordSpec with Matchers with GenericRecordEventFixture {
  import GenericRecordFieldExtractionSupport._

  "getField" should {
    "Extract field of type String from a single nested property" in {
      record.getField[String]("header/correlationId") shouldBe "CORRELATION_ID"
    }

    "fail for empty path" in {
      intercept[IllegalArgumentException] {
        record.getField[String]("")
      }
    }
  }


}
