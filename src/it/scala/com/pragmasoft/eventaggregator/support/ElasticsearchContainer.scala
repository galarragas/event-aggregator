package com.pragmasoft.eventaggregator.support

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.joda.time.DateTime
import org.elasticsearch.common.joda.time.format.DateTimeFormat
import org.elasticsearch.indices.{IndexAlreadyExistsException, IndexMissingException}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait ElasticsearchContainer extends BeforeAndAfterEach with BeforeAndAfterAll {
  self: Suite =>

  val elasticsearch = ElasticsearchServer.start()

  def elasticsearchClient: Client = elasticsearch.client
  def elasticSearchConnectionUrl = {
    s"http://localhost:${elasticsearch.httpPort}"
  }

  case class TypeMapping(typeName: String, mappingJson: Option[String])

  protected lazy val EventsIndexPrefix = "events-"
  protected lazy val EventsIndex = {
    // Run this tests around midnight and they might fail. Better to rerun than making the test logic too complex
    val dateTimeFormat = DateTimeFormat.forPattern("YYYY.MM.dd")
    val today = dateTimeFormat.print(DateTime.now())

    s"$EventsIndexPrefix$today"
  }

  val indexes: Map[String, Iterable[TypeMapping]] = Map(
    EventsIndex -> Seq()
  )

  override def beforeEach() = {
    dropIndexes()
    createIndexes()
    super.beforeEach()
  }

  override def afterEach() = {
    super.afterEach()
    dropIndexes()
  }

  override def beforeAll() = {
    super.beforeAll()
  }

  override def afterAll() = {
    super.afterAll()
  }

  private def dropIndexes(): Unit = for ((index, _) <- indexes) try {
    val _ = elasticsearch.deleteAndWaitForIndex(index)
    eventually {
      val exists = elasticsearch.client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet
      !exists.isExists
    }
  } catch {
    case ignored: IndexMissingException =>
      ()
  }

  private def createIndexes(): Unit = for ((index, typeMappings) <- indexes) {
    val createIndexRequest = new CreateIndexRequest(index)

    for (TypeMapping(typeName, Some(mappingJson)) <- typeMappings) {
      createIndexRequest.mapping(typeName, mappingJson)
    }

    try {
      val _ = elasticsearch.client.admin.indices.create(createIndexRequest).actionGet
    } catch {
      case ignored: IndexAlreadyExistsException =>
        ()
    }
  }


}
