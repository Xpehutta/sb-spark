import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import scala.collection.mutable.Map

object filter {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Lab04_filter")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    val topicName = spark
      .sparkContext
      .getConf
      .getOption("spark.filter.topic_name")

    val offset = spark
      .sparkContext
      .getConf
      .getOption("spark.filter.offset")

    val outputDirectory = spark
      .sparkContext
      .getConf
      .getOption("spark.filter.output_dir_prefix")
      .get

    var kafkaParams: Map[String,String] = Map("kafka.bootstrap.servers" -> "spark-master-1:6667")

    if(topicName.isDefined) {
      kafkaParams += ("subscribe" -> topicName.get)
    }


    if(offset.isDefined && offset.get != "earliest") {
      val offset_str: String = "{\"" + topicName + "\":{\"0\":" + offset.get + "}}"
      kafkaParams += ("startingOffsets" -> offset_str)
    }
    println("kafkaParams -> "+ kafkaParams)
    val df = spark.read
      .format("kafka")
      .options(kafkaParams).load
    import spark.implicits._
    val jsonString: Dataset[String] = df.select(col("value").cast("string")).as[String]
    val VisitsKafkaDF = spark.read.json(jsonString)


    val BuyVisitsDF = VisitsKafkaDF //VisitsDF
      .filter(col("event_type")==="buy")
      .withColumn("date", from_unixtime(col("timestamp")/1000,"yyyymmdd"))
      .withColumn("p_date", from_unixtime(col("timestamp")/1000,"yyyymmdd"))

    BuyVisitsDF
      .write
      .format("json")
      .partitionBy("p_date")
      .mode("overwrite")
      .save(outputDirectory + "/buy")

    val ViewVisitsDF = VisitsKafkaDF //VisitsDF
      .filter(col("event_type")==="view")
      .withColumn("date", from_unixtime(col("timestamp")/1000,"yyyyMMdd"))
      .withColumn("p_date", from_unixtime(col("timestamp")/1000,"yyyyMMdd"))

    ViewVisitsDF
      .write
      .format("json")
      .partitionBy("p_date")
      .mode("overwrite")
      .save(outputDirectory + "/view")
  }
}
