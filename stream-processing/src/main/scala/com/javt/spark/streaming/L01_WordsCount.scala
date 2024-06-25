package com.javt.spark.streaming

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

// 1. Before to start, please delete chk-point-dir/L01 folder
// 2. Open a console, and type: nc -lk 9999
// 3. Execute this program
// 4. within opened console, start typing
object L01_WordsCount extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Lesson 01: Streaming Words Count")
      .master("local[3]")
      .getOrCreate()

    val linesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()


    // linesDF.printSchema()

    val wordsDF = linesDF.select(expr("explode(split(value, ' ')) as word"))
    val countsDF = wordsDF.groupBy("word").count()

    val wordCountQuery = countsDF.writeStream
      .format("console")
      .option("checkpointLocation", "chk-point-dir/L01")
      .outputMode("complete")
      .start()

    logger.info("Listening to localhost:9999")
    wordCountQuery.awaitTermination()

  }
}
