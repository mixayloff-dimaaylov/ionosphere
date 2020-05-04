package com.infocom.examples.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import Functions._
import org.apache.spark.sql.SparkSession

object PowerCalculations {
  def runJob(spark: SparkSession): Any = {
    val sc = spark.sqlContext
    import sc.implicits._

    val csv = sc.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:/tmp/test.csv")

    val largeWindow = Window.partitionBy("sat").orderBy("time").rangeBetween(-LARGE_WINDOW_SIZE, LARGE_WINDOW_SIZE)
    //       .withColumn("s4", stddev_pop($"Value").over(wlarge) / avg($"Value").over(wlarge))

    val data = csv.select(
      $"Time",
      $"Groups",
      $"Value",
      s4intensity(avg(pow($"Value", 2)).over(largeWindow), pow(avg($"Value").over(largeWindow), 2)).as("S4")
    )

    data.write.option("header", "true").csv("C:/tmp/s4bypower2")
  }
}
