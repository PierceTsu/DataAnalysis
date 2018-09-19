package com.pierce.data.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[2]").setAppName("DataFrameApp")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val peopleDF = sqlContext.read.json("src/data/people.json")
    peopleDF.show()
  }
}
