package com.javt.spark.streaming

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

// Please, before to run this app, be sure:
// 1. run zookeeper: zookeeper-server.sh
// 2. run kafka broker: kafka-server-start.sh
// 3. create `L09-Sensor` topic: kafka-topics.sh --create --topic L09-Sensor --bootstrap-server localhost:9092
//                                  --replication-factor 1 --partitions 1
// 4. start kafka producer: kafka-console-producer.sh --topic L09-sensor --bootstrap-server localhost:9092
// 5. Run the `L09_SlidingWindow` application
object L09_SlidingWindow extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Lesson 09: Sliding Window Demo")
      .master("local[3]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    val invoiceSchema = StructType(List(
     StructField("CreatedTime", StringType),
     StructField("Reading", DoubleType)
    ))

    val kafkaSourceDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "L09-Sensor")
      .option("startingOffsets", "earliest")
      .load()

    val valueDF = kafkaSourceDF.select(
      col("key").cast("string").alias("SensorID"),
      from_json(col("value").cast("string"), invoiceSchema).alias("value")
    )

    val sensorDF = valueDF.select("SensorID", "value.*")
      .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))

    val aggDF = sensorDF
      .withWatermark("CreatedTime", "30 minute")
      .groupBy(
        col("SensorID"),
        window(col("CreatedTime"), "15 minute", "5 minute")
      )
      .agg(
        max("Reading").alias("MaxReading")
      )

    val outputDF = aggDF.select("SensorID", "window.start", "window.end", "MaxReading")

    val windowQuery = outputDF.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir/L09")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Counting Invoices")
    windowQuery.awaitTermination()

  }

}
