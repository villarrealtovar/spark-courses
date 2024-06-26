package com.javt.spark.streaming

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

// Please, before to run this app, be sure:
// 1. run zookeeper: zookeeper-server.sh
// 2. run kafka broker: kafka-server-start.sh
// 3. create `L04-invoices` topic: kafka-topics.sh --create --topic L04-invoices --bootstrap-server localhost:9092
//                                  --replication-factor 1 --partitions 1
// 4. create `L04-notifications` topic: kafka-topics.sh --create --topic L04-notifications --bootstrap-server localhost:9092
//                                  --replication-factor 1 --partitions 1
// 5. start kafka producer: kafka-console-producer.sh --topic L04-invoices --bootstrap-server localhost:9092
// 6. Run the `L04_KafkaSink` application
// 7. start consumer: kafka-console-consumer.sh --topic L04-notifications --from-beginning --bootstrap-server localhost:9092
object L04_KafkaSink extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Lesson 04: Kafka Sink")
      .master("local[3]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    val schema = StructType(List(
      StructField("InvoiceNumber", StringType),
      StructField("CreatedTime", LongType),
      StructField("StoreID", StringType),
      StructField("PosID", StringType),
      StructField("CashierID", StringType),
      StructField("CustomerType", StringType),
      StructField("CustomerCardNo", StringType),
      StructField("TotalAmount", DoubleType),
      StructField("NumberOfItems", IntegerType),
      StructField("PaymentMethod", StringType),
      StructField("CGST", DoubleType),
      StructField("SGST", DoubleType),
      StructField("CESS", DoubleType),
      StructField("DeliveryType", StringType),
      StructField("DeliveryAddress", StructType(List(
        StructField("AddressLine", StringType),
        StructField("City", StringType),
        StructField("State", StringType),
        StructField("PinCode", StringType),
        StructField("ContactNumber", StringType)
      ))),
      StructField("InvoiceLineItems", ArrayType(StructType(List(
        StructField("ItemCode", StringType),
        StructField("ItemDescription", StringType),
        StructField("ItemPrice", DoubleType),
        StructField("ItemQty", IntegerType),
        StructField("TotalValue", DoubleType)
      )))),
    ))

    val kafkaSourceDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "L04-invoices")
      .option("startingOffsets", "earliest")
      .load()

    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), schema).alias("value"))
    // valueDF.show()

    val notificationDF = valueDF.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount")
      .withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2"))

    // notificationDF.show()

    val kafkaTargetDF = notificationDF.selectExpr("InvoiceNumber as key",
    """
        |to_json(named_struct('CustomerCardNo', CustomerCardNo,
        |'TotalAmount', TotalAmount,
        |'EarnedLoyaltyPoints', EarnedLoyaltyPoints)) as value
        |""".stripMargin)

    val notificationWriterQuery = kafkaTargetDF
      .writeStream
      .queryName("Notification Writer")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "L04-notifications")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir/L04")
      .start()

    logger.info("Listening and writing to Kafka")
    notificationWriterQuery.awaitTermination()
  }

}
