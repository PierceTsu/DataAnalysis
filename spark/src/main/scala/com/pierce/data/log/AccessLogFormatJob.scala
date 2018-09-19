package com.pierce.data.log

import com.pierce.data.log.util.DateUtil
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object AccessLogFormatJob {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("AccessLogCleanJob")
    val sc = SparkContext.getOrCreate(sparkConf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val accessLogRDD = sc.textFile("src/data/10000_access.log")

    val schema = StructType(
        StructField("time", StringType) ::
        StructField("url", StringType) ::
        StructField("traffic", LongType) ::
        StructField("ip", StringType) :: Nil
    )

    val rowRDD = accessLogRDD
      .map(_.split(" "))
      .map(attributes => Row(
        DateUtil.parseTime(attributes(3) + " " + attributes(4)),
        attributes(11).replace("\"", ""),
        attributes(9).toLong,
        attributes(0)
      ))
    val accessDF = sqlContext.createDataFrame(rowRDD, schema)
    accessDF.show()
    sc.stop()
  }
}
