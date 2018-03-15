package com.spark

import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions._

object DateChanger {

  val purl = "jdbc:postgresql://myrdsinstance.cyqhcbzbuvov.us-east-2.rds.amazonaws.com/myRDSInstance?user=postgres&password=postgres"
  val url = "jdbc:postgresql://localhost/wiki?user=postgres&password=postgres"

  def main(args: Array[String]){

    // Load postgresql jdbc driver
    Class.forName("org.postgresql.Driver")

    /*no need to  explicating creating SparkConf, SparkContext or SQLContext, as they’re encapsulated within the SparkSession.*/
    val sparkSession = SparkSession.builder().appName("SparkSessionWiki").getOrCreate()


    val customSchema = StructType(Array(
      StructField("RetailStoreID", StringType, nullable = true),
      StructField("WorkstationID", StringType, nullable = true),
      StructField("EndDateTime", StringType, nullable = true),
      StructField("CurrencyCode", StringType ,nullable = true),
      StructField("BusinessDayDate", StringType, nullable = true)))


      /* Spark automatically reads the schema from the database table and maps its types back to Spark SQL types. */
    val dataFrame = sparkSession.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "Transaction")
      .schema(customSchema)
      .load("testing.xml")
   //   .load("hewiki-20160203-pages-articles-multistream.xml")

    val clnDataFrame = dataFrame.select(
      dataFrame.columns.map {
        case year @ "EndDateTime" => dataFrame(year).cast(DateType).as(year.toLowerCase())
        case make @ "CurrencyCode" => functions.lower(dataFrame(make)).as(make.toLowerCase)
        case other         => dataFrame(other).alias(other.toLowerCase())
      }: _*
    )

    val clnDataFrameWithId = clnDataFrame.withColumn("id",monotonically_increasing_id())

    clnDataFrameWithId.write.mode(SaveMode.Append).option("driver", "org.postgresql.Driver").jdbc(purl,"wiki_data",JDBCConnector.connectionCfg)

  }
}
