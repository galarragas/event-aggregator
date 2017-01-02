package com.pragmasoft.eventaggregator.support

import java.net.ServerSocket
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.NodeBuilder._

object ElasticsearchServer {
  def start(): ElasticsearchServer = {
    val server = new ElasticsearchServer()
    server.start()
    server
  }
}

class ElasticsearchServer {

  private val clusterName = "testElasticsearch"
  private val dataDir = Files.createTempDirectory("elasticsearch_data_").toFile
  private val homeDir = Files.createTempDirectory("elasticsearch_home_").toFile

  val httpPort = findRandomOpenPortOnAllLocalInterfaces()
  val tcpPort = findRandomOpenPortOnAllLocalInterfaces()

  private val settings = Settings.settingsBuilder
    .put("path.home", dataDir.toString)
    .put("path.data", dataDir.toString)
    .put("http.port", httpPort)
    .put("transport.tcp.port", tcpPort)
    .put("cluster.name", clusterName)
    .build

  private def findRandomOpenPortOnAllLocalInterfaces() : Int = {
    var socket: ServerSocket = null
    try {
      socket = new ServerSocket(0)
      socket.getLocalPort()
    } finally {
      if(socket != null)
        socket.close()
    }
  }

  private lazy val node = nodeBuilder().local(true).settings(settings).build
  def client: Client = node.client

  def start(): Unit = {
    node.start()
  }

  def stop(): Unit = {
    node.close()

    try {
      FileUtils.forceDelete(homeDir)
    } catch {
      case e: Exception =>
        Console.err.println("Elasticsearch homeDir cleanup failed", e)
    }

    try {
      FileUtils.forceDelete(dataDir)
    } catch {
      case e: Exception =>
        Console.err.println("Elasticsearch dataDir cleanup failed", e)
    }
  }

  def deleteAndWaitForIndex(index: String): Unit = {
    client.admin.indices.prepareDelete(index).execute.actionGet()
  }

  def createAndWaitForIndex(index: String): Unit = {
    client.admin.indices.prepareCreate(index).execute.actionGet()
    client.admin.cluster.prepareHealth(index).setWaitForActiveShards(1).execute.actionGet()
  }
}
