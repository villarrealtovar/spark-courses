package com.javt.spark.streaming

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.types._

// Please, before to run this app, be sure:
// 1. run zookeeper: zookeeper-server.sh
// 2. run kafka broker: kafka-server-start.sh
// 3. create `L05-invoices` topic: kafka-topics.sh --create --topic L05-invoices --bootstrap-server localhost:9092
//                                  --replication-factor 1 --partitions 1
// 4. create `L05-notifications` topic: kafka-topics.sh --create --topic L05-notifications --bootstrap-server localhost:9092
//                                  --replication-factor 1 --partitions 1
// 5. start kafka producer: kafka-console-producer.sh --topic L05-invoices --bootstrap-server localhost:9092
// 6. Run the `L05_MultiQuery` application
// 7. start consumer: kafka-console-consumer.sh --topic L05-notifications --from-beginning --bootstrap-server localhost:9092
object L05_MultiQuery extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Lesson 05: MultiQuery")
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
      .option("subscribe", "L05-invoices")
      .option("startingOffsets", "earliest")
      .load()

    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), schema).alias("value"))

    val notificationDF = valueDF.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount")
      .withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2"))

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
      .option("topic", "L05-notifications")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir/L05/notify")
      .start()


    val explodeDF = valueDF.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID", "value.PosID",
      "value.CustomerType", "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
      "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")

    val flattenedDF = explodeDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")

    val invoiceWriterQuery = flattenedDF.writeStream
      .queryName("Flattened Invoice Writer")
      .format("json")
      .outputMode("append")
      .option("path", "output/L05")
      .option("checkpointLocation", "chk-point-dir/L05/flatten")
      .start()

    logger.info("Waiting for Queries")
    spark.streams.awaitAnyTermination()
  }

}
