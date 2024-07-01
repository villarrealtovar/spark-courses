package com.javt.spark.streaming

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// Please, before to run this app, be sure:
// 1. run zookeeper: zookeeper-server.sh
// 2. run kafka broker: kafka-server-start.sh
// 3. create `L08-trades` topic: kafka-topics.sh --create --topic L08-trades --bootstrap-server localhost:9092
//                                  --replication-factor 1 --partitions 1
// 4. start kafka producer: kafka-console-producer.sh --topic L08-trades --bootstrap-server localhost:9092
// 5. Run the `L08_TumblingWindow` application
object L08_TumblingWindow extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Lesson 08: Tumbling Window Demo")
      .master("local[3]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    val stockSchema = StructType(List(
     StructField("CreatedTime", StringType),
     StructField("Type", StringType),
     StructField("Amount", IntegerType),
     StructField("BrokerCode", StringType)
    ))

    val kafkaSourceDF = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "L08-trades")
      .option("startingOffsets", "earliest")
      .load()

    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), stockSchema).alias("value"))
    // valueDF.printSchema()

    val tradeDF = valueDF.select("value.*")
      .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end"))
      .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))

    val windowAggDF = tradeDF
      .withWatermark("CreatedTime", "30 minutes")
      .groupBy(window(col("CreatedTime"), "15 minute"))
      .agg(
        sum("Buy").alias("TotalBuy"),
        sum("Sell").alias("TotalSell")
      )

    // windowAggDF.printSchema()

    val outputDF = windowAggDF.select("window.start", "window.end", "TotalBuy", "TotalSell")

    val runningTotalWindow = Window.orderBy("end")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val finalOutputDF = outputDF
      .withColumn("RTotalBuy", sum("TotalBuy").over(runningTotalWindow))
      .withColumn("RTotalSell", sum("TotalSell").over(runningTotalWindow))
      .withColumn("NetValue", expr("RTotalBuy - RTotalSell"))

    finalOutputDF.show(false)

   /* val windowQuery = outputDF.writeStream
      .format("console")
      .outputMode("update")
      .option("checkpointLocation", "chk-point-dir/L08")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("L08: Tumbling Window")
    windowQuery.awaitTermination()*/

  }

}
