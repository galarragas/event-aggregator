package com.pragmasoft.eventaggregator.streams

import java.time.Clock

import org.elasticsearch.common.joda.time.DateTime
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class DayPartitionedElasticSearchIndexNameProviderSpec extends WordSpec with Matchers with MockitoSugar {

  class TestIndexNameProvider(override val elasticSearchIndexPrefix: String) extends DayPartitionedElasticSearchIndexNameProvider {
    override val clock = mock[Clock]
  }


  "DayPartitionedElasticSearchIndexNameProvider" should {

    "create an index name dependent on current date in the format prefixYYYY.MM.dd" in {

      val testProvider = new TestIndexNameProvider("prefix=")

      when(testProvider.clock.millis()).thenReturn {
        new DateTime()
          .withYear(2015)
          .withMonthOfYear(12)
          .withDayOfMonth(21)
          .getMillis
      }

      testProvider.elasticSearchIndex should be("prefix=2015.12.21")
    }

  }

}
