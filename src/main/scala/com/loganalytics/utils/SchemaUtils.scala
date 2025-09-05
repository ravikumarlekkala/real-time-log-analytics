package com.loganalytics

import org.apache.spark.sql.types._

object SchemaUtils {

  /** Kafka / Mongo JSON schema */
  val logSchema: StructType = StructType(Seq(
    StructField("timestamp", StringType, true),
    StructField("level", StringType, true),
    StructField("service", StringType, true),
    StructField("path", StringType, true),
    StructField("status", IntegerType, true),
    StructField("latencyMs", LongType, true),
    StructField("msg", StringType, true),
    StructField("userId", StringType, true),
    StructField("host", StringType, true),
    StructField("ip", StringType, true),
    StructField("requestId", StringType, true),
    StructField("sessionId", StringType, true)
  ))

  /** Normalized schema after transformation */
  val rawUnifiedSchema: StructType = StructType(Seq(
    StructField("event_time", TimestampType, true),
    StructField("level", StringType, true),
    StructField("service", StringType, true),
    StructField("path", StringType, true),
    StructField("status", IntegerType, true),
    StructField("latency_ms", LongType, true),
    StructField("msg", StringType, true),
    StructField("user_id", StringType, true),
    StructField("host", StringType, true),
    StructField("ip", StringType, true),
    StructField("request_id", StringType, true),
    StructField("session_id", StringType, true)
  ))

}
