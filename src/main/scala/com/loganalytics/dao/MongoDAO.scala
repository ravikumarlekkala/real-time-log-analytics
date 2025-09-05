package com.loganalytics.dao

import com.loganalytics.config.AppConfig
import com.loganalytics.SchemaUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MongoDAO {

  def loadLogs(spark: SparkSession): DataFrame = {
    println(s"[MongoDAO] Attempting to load '${AppConfig.mongoDb}.${AppConfig.mongoColl}'")

    val df = spark.read
      .format("mongodb")
      .option("spark.mongodb.connection.uri", AppConfig.mongoUri)
      .option("database", AppConfig.mongoDb)
      .option("collection", AppConfig.mongoColl)
      .schema(SchemaUtils.logSchema) // enforce schema
      .load()

    println(s"[MongoDAO] Count = ${df.count()}")
    df.printSchema()
    df.show(5, truncate = false)

    df
  }


  def buildAggregates(df: DataFrame, withWatermark: Boolean, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val watermarkDur = "5 minutes"
    val windowDur    = "1 minute"
    val slideDur     = "1 minute"

    val base = if (withWatermark) df.withWatermark("event_time", watermarkDur) else df

    base
      .groupBy(
        window($"event_time", windowDur, slideDur),
        $"service"
      )
      .agg(
        count(lit(1)).as("events"),
        sum(when($"status" >= 500, 1).otherwise(0)).cast("long").as("errors"),
        avg($"latency_ms").cast("double").as("latency_ms")
      )
      .select(
        col("window.start").as("windowStart"),
        col("window.end").as("windowEnd"),
        col("service"),
        col("events").cast("long"),
        col("errors").cast("long"),
        col("latency_ms").cast("double")
      )
  }
}
