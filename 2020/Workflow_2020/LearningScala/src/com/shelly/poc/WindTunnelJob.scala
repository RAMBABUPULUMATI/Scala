package com.shelly.poc

import org.apache.spark.sql.SparkSession

object WindTunnelJob {
  def main(args: Array[String]) {
    val logFile = "/dev/ecfd/spark_poc/2015-summary.csv" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}