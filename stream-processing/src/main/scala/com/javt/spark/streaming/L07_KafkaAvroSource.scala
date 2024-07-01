package com.javt.spark.streaming

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.functions.{col, expr, struct, sum, to_json}

import java.nio.file.{Files, Paths}

// Please, before to run this app, be sure:
// 1. execute L06_kafkaAvroSink application before (and all its setup) for publishing some avro invoices
//    in `L06-invoices-items` topic
// 2. create `L07-customer-rewards` topic: kafka-topics.sh --create --topic L07-customer-rewards --bootstrap-server localhost:9092
//                                  --replication-factor 1 --partitions 1
// 3. Run the `L07_KafkaAvroSource` application

object L07_KafkaAvroSource extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Lesson 07: Kafka Avro Source")
      .master("local[3]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    val kafkaSourceDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "L06-invoices-items") // we are going to read the L06-invoices-items topic which has avro format
      .option("startingOffsets", "earliest")
      .load()

    val avroSchema = new String(Files.readAllBytes(Paths.get("input/L07/schema/invoice-items")))
    val valueDF = kafkaSourceDF.select(from_avro(col("value"), avroSchema).alias("value"))

    val rewardsDF = valueDF.filter("value.CustomerType == 'PRIME'")
      .groupBy("value.CustomerCardNo")
      .agg(
        sum("value.TotalValue").alias("TotalPurchase"),
        sum(expr("value.TotalValue*0.2").cast("integer")).alias("AggregateRewards")
      )

    val kafkaTargetDF = rewardsDF.select(expr("CustomerCardNo as key"),
      to_json(struct("TotalPurchase", "AggregateRewards")).alias("value")
    )

    val rewardsWriterQuery = kafkaTargetDF
      .writeStream
      .queryName("Rewards Writer")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "L07-customer-rewards")
      .outputMode("update")
      .option("checkpointLocation", "chk-point-dir/L07")
      .start()

    logger.info("L07: Rewards Write Query")
    rewardsWriterQuery.awaitTermination()
  }

}
