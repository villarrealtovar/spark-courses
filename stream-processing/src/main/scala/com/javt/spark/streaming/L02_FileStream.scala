package com.javt.spark.streaming

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger

object L02_FileStream extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Lesson 03: File Stream")
      .master("local[3]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()

    val rawDF = spark.readStream
      .format("json")
      .option("path", "input/L02") // `input` is the folder's name
      .option("maxFilesPerTrigger", 1)
      .load()

    // rawDF.printSchema()

    val explodeDF = rawDF.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
    "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
    "DeliveryAddress.State", "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")

    // explodeDF.printSchema()

    val flattenedDF = explodeDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .drop("LineItem")

    val invoiceWriterQuery = flattenedDF.writeStream
      .format("json")
      .option("path", "output/L02")
      .option("checkpointLocation", "chk-point-dir/L02")
      .outputMode("append")
      .queryName("Flattened Invoice Writer")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Flattened Invoice Writer started")
    invoiceWriterQuery.awaitTermination()

  }

}
