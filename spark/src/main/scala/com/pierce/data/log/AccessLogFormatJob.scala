package com.pierce.data.log

import com.pierce.data.log.util.DateUtil
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

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
        StructField("day", StringType) ::
        StructField("ip", StringType) :: Nil
    )

    val rowRDD = accessLogRDD
      .map(_.split(" "))
      .map(attributes => {
        val time = DateUtil.parseTime(attributes(3) + " " + attributes(4))
        Row(
          time,
          attributes(11).replace("\"", ""),
          attributes(9).toLong,
          time.substring(0, 10).replace("-", ""),
          attributes(0)
      )})
    val accessDF = sqlContext.createDataFrame(rowRDD, schema)
    accessDF.filter("url != '-'").show(false)
    accessDF.coalesce(1)
      .filter("url != '-'")
      .write
      .format("parquet")
      .partitionBy("day")
      .mode(SaveMode.Overwrite) //覆盖方式
      .save("src/data/output")
    sc.stop()
  }
}
