package com.cloudera

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, Row}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types._
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * @author ${sdunkler@cloudera.com}
 */
object EventSessionJoiner {

  val KAFKA = "kafka"
  val TOPIC = "topic"
  val VALUE = "value"
  val NAME = "name"
  val SUBSCRIBE = "subscribe"
  val CHECKPOINT_LOCATION = "checkpointLocation"
  val TIMESTAMP = "timestamp"
  val STRING = "string"
  val DOUBLE = "double"
  val INTEGER = "integer"
  val TS = "ts"
  val COLUMNS = "columns"
  val KAFKABOOTSTRAPSERVERS = "kafka.bootstrap.servers"
  val KAFKASECURITYPROTOCOL = "kafka.security.protocol"
  val KAFKA_BOOTSTRAP_SERVERS = "kafka_bootstrap_servers"
  val KAFKA_SECURITY_PROTOCOL = "kafka_security_protocol"
  val KAFKA_INPUT_STREAMS = "kafka_input_streams"
  val DELAY_THRESHOLD = "delay_threshold"
  val JOIN_CONDITION = "join_condition"
  val OUTPUT_COLUMNS = "output_columns"
  val OUTPUT_FORMAT = "output_format"
  val KAFKA_OUTPUT_TOPIC = "kafka_output_topic"

  def main(args : Array[String]) {

    if (args.length == 0 || args.length > 1) {
      println("You need to specify exactly one command line argument: path to config file in HDFS -> 'hdfs:///my/path'")
    }
    val filename = args(0)

    val spark = SparkSession
      .builder
      .appName("EventSessionJoiner")
      .getOrCreate()
    import spark.implicits._

    //spark.sparkContext.setLogLevel("DEBUG")

    // Read properties from config file
    val path = new Path(filename)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val inputStream = fs.open(path)
    val yaml = new Yaml()
    val properties = yaml.load(inputStream).asInstanceOf[java.util.Map[String, Any]]
    val kafkaBootstrapServers = properties.get(KAFKA_BOOTSTRAP_SERVERS).toString
    val kafkaSecurityProtocol = properties.get(KAFKA_SECURITY_PROTOCOL).toString
    val kafkaInputStreams = properties.get(KAFKA_INPUT_STREAMS).asInstanceOf[java.util.List[java.util.Map[String,Any]]].toList
    val outputColumns = properties.get(OUTPUT_COLUMNS).asInstanceOf[java.util.List[String]].toList
    val outputFormat = properties.get(OUTPUT_FORMAT).toString
    val kafkaOutputTopic = properties.get(KAFKA_OUTPUT_TOPIC).toString
    val checkpointLocation = properties.get(CHECKPOINT_LOCATION).toString

    // Define and create input streams
    val eventStreams = new mutable.HashMap[String, Dataset[Row]]()

    for (streamDefinition <- kafkaInputStreams) {
      val streamName = streamDefinition.get(NAME).toString
      val columns = streamDefinition.get(COLUMNS).asInstanceOf[java.util.List[String]].toList
      val delayThreshold = streamDefinition(DELAY_THRESHOLD).toString

      // create stream
      val stream =
        spark.readStream.format(KAFKA)
          .option(KAFKABOOTSTRAPSERVERS, kafkaBootstrapServers)
          .option(KAFKASECURITYPROTOCOL, kafkaSecurityProtocol)
          .option(SUBSCRIBE, streamDefinition.get(TOPIC).toString)
          .load()

      // create schema
      var schema = new StructType()
      schema = schema.add(TIMESTAMP, StringType)
      for (column <- columns) {
        schema = schema.add(column, StringType)
      }

      // apply schema
      val cleanStream = stream.selectExpr("CAST(value AS STRING)")
        .select(from_json($"value", schema) as "value")
        .select(
          $"value.*",
          ($"value.timestamp".cast(STRING).cast(DOUBLE) /1000).cast(INTEGER).cast(TIMESTAMP).alias(TS))
        .withWatermark(TS, delayThreshold).alias(streamName)

      eventStreams.put(streamName, cleanStream)

    }

    // initialize join
    var joinStream = eventStreams(kafkaInputStreams.head.get(NAME).toString)

    // join subsequent streams
    for (streamDefinition <- kafkaInputStreams.tail) {


      joinStream = joinStream.join(
        eventStreams(streamDefinition.get(NAME).toString),
        expr(streamDefinition.get(JOIN_CONDITION).toString)
      )
    }

    // apply output schema
    joinStream = joinStream.select(outputColumns.map(c => col(c)): _*)

    // write to kafka
    val kafkaOutput = joinStream
      .select(to_json(struct($"*")).alias(VALUE))
      .writeStream
      .format(outputFormat)
      .option(KAFKABOOTSTRAPSERVERS, kafkaBootstrapServers)
      .option(TOPIC, kafkaOutputTopic)
      .option(KAFKASECURITYPROTOCOL, kafkaSecurityProtocol)
      .option(CHECKPOINT_LOCATION, checkpointLocation)
      .start()

    kafkaOutput.awaitTermination()

  }

}
