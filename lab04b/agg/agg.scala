import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._


val mySchema = new StructType()
             .add("category", StringType)
             .add("event_type", StringType)
             .add("item_id", StringType)
             .add("item_price", LongType)
             .add("timestamp", LongType)
             .add("uid", StringType)

val kafkaParams = Map(
        "kafka.bootstrap.servers" -> "spark-master-1:6667",
        "subscribe" -> "nikolay_abramov"
    )

val sdf = spark.readStream.format("kafka").options(kafkaParams).load
val structCol = from_json(col("value").cast("string"), mySchema)
val parsedSdf = sdf
                  .select(structCol.alias("s")).select(col("s.*"))
    
                  .withColumn("revenue_price", 
                                   when(col("event_type")==="buy",col("item_price"))
                                   .otherwise(lit(0)))
                  .withColumn("visitors_flag", 
                                   when(col("uid").isNotNull,lit(1))
                                   .otherwise(lit(0)))
                  .withColumn("purchases_flag", 
                                   when(col("event_type")==="buy",lit(1))
                                   .otherwise(lit(0)))
                  .withColumn("timestamp_converted", ($"timestamp" / 1000).cast(TimestampType))

             
def uData: DataFrame = parsedSdf
    .withWatermark("timestamp_converted", "1 hour")
    .groupBy(window($"timestamp_converted", "1 hour", "1 hour"))
    .agg(
      sum($"revenue_price").alias("revenue"),
      sum($"visitors_flag").alias("visitors"),
      sum($"purchases_flag").alias("purchases")  
    )
    .withColumn(
      "aov",
      $"revenue" / $"purchases"
    )
    .select(
      unix_timestamp($"window.start").as("start_ts"),
      unix_timestamp($"window.end").as("end_ts"),
      $"revenue",
      $"visitors",
      $"purchases",
      $"aov"
    )

val data = uData
      .selectExpr("cast(start_ts as string) as key", "to_json(struct(*)) as value")
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("kafka")
      .option("checkpointLocation", s"user/nikolay.abramov/tmp/chk/lab04")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("topic", "nikolay_abramov_lab04b_out")     
      .outputMode("update")
      .start()

data.awaitTermination()

