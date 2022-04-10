%AddDeps com.datastax.spark spark-cassandra-connector_2.11 2.3.3 
%AddDeps org.elasticsearch elasticsearch-spark-20_2.11 8.0.1
%AddDeps org.postgresql postgresql 42.2.12

import org.apache.spark.sql._ 
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.functions._ 

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

import org.apache.spark.sql.cassandra._ 


import scala.util.Try 
import java.sql.DriverManager
import java.sql.Connection

val sparkCassandra: SparkSession = SparkSession 
      .builder() 
      .appName("Lab03_Cassandra") 
      .config("spark.master", "local[1]") 
      .config("spark.sql.catalog.csdra", "com.datastax.spark.connector.datasource.CassandraCatalog") 
      .config("spark.cassandra.connection.host", "10.0.0.31") 
      .config("spark.cassandra.connection.port", "9042") 
      .getOrCreate() 
 
val userDF = sparkCassandra.read 
      .format("org.apache.spark.sql.cassandra") 
      .options(Map( "table" -> "clients", "keyspace" -> "labdata")) 
      .load()
    
SparkSession.clearActiveSession()
SparkSession.clearDefaultSession()


val sparkJSON: SparkSession = SparkSession 
      .builder() 
      .appName("Lab03_JSON") 
      .config("spark.master", "local[1]")
      .getOrCreate()

val WebsiteLog = sparkJSON
      .read
      .json("hdfs:///labs/laba03/weblogs.json")
      .select(col("uid"), explode(col("visits")).as("visitArr"))
      .select(  col("uid")
              , col("visitArr.timestamp")
              , col("visitArr.url")
             )
     .withColumn("url_domain", regexp_extract(col("url"), raw"^(?:https?:\/\/)?(?:[^@\n]+@)?(?:www.)?([^:\/\n?]+)", 1))

SparkSession.clearActiveSession()
SparkSession.clearDefaultSession()

val sparkES: SparkSession = SparkSession 
   .builder()
   .appName("Lab03_ES")
   .master("local[*]")
   .getOrCreate()

val options = Map("es.nodes" -> "10.0.0.31:9200", "pushdown" -> "true")


val ESDF = sparkES
.read
.format("org.elasticsearch.spark.sql")
.options(options)
.load("visits/_doc")


SparkSession.clearActiveSession()
SparkSession.clearDefaultSession()


val sparkPostgre: SparkSession = SparkSession 
   .builder()
   .appName("Lab03_Postgre")
   .master("local[*]")
   .getOrCreate()

//val driver = "org.postgresql.Driver"

val PostgeDF = sparkPostgre
  .read
  .format("jdbc")
  .option("driver", "org.postgresql.Driver")
  .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
  .option("user", "nikolay_abramov")
  .option("password", "iujEDq9T")
  .option("dbtable", "domain_cats")
  .option("dbschema", "labdata")
  .load()

val ESModifiedDF = ESDF
    .filter(ESDF("uid").isNotNull)
    .withColumn("shop_cat", lower(concat(lit("shop_"),regexp_replace(col("category"), "-", "_"))))
    .withColumnRenamed("uid","shop_uid")
    .select("shop_uid", "shop_cat")

val UserModifiedDF = userDF 
    .withColumn("age_cat"
     , when(col("age") <= 24,"18-24")
      .when(col("age") > 24 && col("age") <= 34,"25-34")
      .when(col("age") > 34 && col("age") <= 44,"35-44")
      .when(col("age") > 44 && col("age") <= 54,"45-54")
      .otherwise(">=55")
               )
    .withColumnRenamed("uid","Uuid")
    .select("Uuid", "gender", "age_cat")


val pivotWebCatDF = WebsiteLog
    .join(PostgeDF,
         WebsiteLog("url_domain") === PostgeDF("domain")
         , "inner")
    .withColumn("web_cat", lower(concat(lit("web_"), col("category"))))
    .groupBy("uid")
    .pivot("web_cat")
    .count()
    .cache()


val pivotShopCatDF = ESModifiedDF
    .groupBy("shop_uid")
    .pivot("shop_cat")
    .count()
    .cache()


val FinalDF = UserModifiedDF
    .join(pivotShopCatDF,
          UserModifiedDF("Uuid") === pivotShopCatDF("shop_uid")
          , "left"
    )
    .join(pivotWebCatDF,
          UserModifiedDF ("Uuid") === pivotWebCatDF("uid") 
          , "left"
        
    )
    .drop("uid")
    .drop("shop_uid")
    .withColumnRenamed("Uuid","uid")
    .cache()


println("Datamart was created")

FinalDF
  .write
  .format("jdbc")
  .option("driver", "org.postgresql.Driver")
  .option("url", "jdbc:postgresql://10.0.0.31:5432/nikolay_abramov")
  .option("user", "nikolay_abramov")
  .option("password", "iujEDq9T")
  .option("dbschema", "nikolay_abramov")  
  .option("dbtable", "clients") 
  .option("truncate", "true")
  .mode("overwrite")
  .save()

println("Datamart was written")


val driver = "org.postgresql.Driver"
val url = "jdbc:postgresql://10.0.0.31:5432/nikolay_abramov"
val username = "nikolay_abramov"
val password = "iujEDq9T"
var connection:Connection = null
connection = DriverManager.getConnection(url, username, password)
val statement = connection.createStatement()
val resultSet = statement.execute("GRANT SELECT ON clients TO labchecker2")
connection.close()
 
println("Datamart granted to labchecker2")


