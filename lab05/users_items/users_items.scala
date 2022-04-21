import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.io.{File, PrintWriter}

object users_items {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("users_items")
      .config("spark.sql.session.timeZone", "UTC")
      // .config("spark.master", "yarn")
      .getOrCreate()


    val conf_update = spark.conf.get("spark.users_items.update", "1")
    val inputDir = spark.conf.get("spark.users_items.input_dir")
    val outputDir = spark.conf.get("spark.users_items.output_dir")

    val buyDF = getDF(spark, inputDir, "buy")

    val viewDF = getDF(spark, inputDir, "view")

    var UnionDataDF = viewDF.union(buyDF)

    if (conf_update.equals("1")) {
      // get current Matrix
      UnionDataDF = UnionDataDF
        .union(getOutputDf(spark, outputDir))
    }

    // calculate date for new matrix
    val newMatriDir = UnionDataDF
      .agg(max(col("p_date")))
      .collect()(0)(0)
      .toString
      .replace("-", "")

    UnionDataDF
      .groupBy(col("uid"))
      .pivot("item_id")
      .agg(count("uid"))
      .na.fill(0)
      .write
      .mode("overwrite")
      .parquet(outputDir + "/" + newMatriDir)
    spark.stop()

  }
  /*
    defined functions
   */
  def getDF(spark: SparkSession, path: String, even_type: String): DataFrame = {
    spark
      .read.format("json")
      .option("path", path + "/" + even_type)
      .load
      .select(col("uid"), col("item_id"), col("p_date"))
      .filter(col("uid").isNotNull)
      .withColumn("item_id", lower(concat(lit(even_type),lit("_"),
        regexp_replace(col("item_id"),"[ -]","_"))
      )
      )

  }
  //-------------------------------------//
  // данные в hdfs не будут меняться, там посмотрим на дату последней матриц
  // корректнее на файловой системе в output dir вычислять
  def getCurrentMatrixDate(spark: SparkSession): String = {
    val hdfsMatrixDataDir = s"/user/nikolay.abramov/visits"
    val hdfsDataDF = spark.read
      .option("header", true)
      .json(hdfsMatrixDataDir + "/*/*/*.json")
      .toDF
    val lastMatrixDate = hdfsDataDF.select(date_format(max((col("timestamp") / 1000).cast("timestamp")), "yyyyMMdd"))
      .collect()(0)(0).toString
    lastMatrixDate
  }

  // get current Matrix for union
  def getCurrentMatrixDF(spark: SparkSession, path: String, currentMatrixDir: String): DataFrame = {
    spark
      .read.format("parquet")
      .option("path", path + "/" + currentMatrixDir)
      .load
  }
  def getOutputDf(spark: SparkSession, outputDir: String): DataFrame = {

    val latestSubdir: String = {
      outputDir.split("://")(0) match {
        case "hdfs" =>
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
          fs.listStatus(new Path(outputDir))
            .filter(_.isDirectory)
            .map(_.getPath.getName)
            .sortWith(_ > _)(0)
        case _ =>
          val file = new File(outputDir.stripPrefix("file:/"))
          file.listFiles
            .filter(_.isDirectory)
            .map(_.getName)
            .sortWith(_ > _)(0)
      }
    }

    val outputDf = spark
      .read.format("parquet")
      .option("path", outputDir + "/" + latestSubdir)
      .load

    val cols = outputDf.columns.filter(_.toLowerCase != "uid")
    val alias_cols = "item_id,value"
    val stack_exp = cols
      .map(x => s"""'${x}',${x}""")
      .mkString(s"stack(${cols.length},", ",", s""") as (${alias_cols})""")

    outputDf
      .select(
        col("uid"),
        expr(s"""${stack_exp}"""),
        lit(latestSubdir).alias("p_date")
      )
      .drop("value")
  }
}
