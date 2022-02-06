package com.infocom.examples.spark

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import Functions._
import java.util.Properties

import org.apache.spark.SparkConf

//noinspection ScalaStyle
object RangeCalculations {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.out.println("Wrong arguments")
      printHelp()
      System.exit(1)
    }

    if (args.length > 3) {
      System.out.println("Extra arguments")
      printHelp()
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Range Calculations")
    val master = conf.getOption("spark.master")

    if (master.isEmpty) {
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder().config(conf).getOrCreate()
    runJob(spark, args(0).toLong, args(1).toLong, args(2))
  }

  def runJob(spark: SparkSession, from: Long, to: Long, sat: String): Unit = {
    val sc = spark.sqlContext
    import sc.implicits._

    val jdbcUri = s"jdbc:clickhouse://st9"
    @transient val jdbcProps = new Properties()
    jdbcProps.setProperty("isolationLevel", "NONE")

    @transient val rankWindow = Window.partitionBy($"tags"("sat")).orderBy("time")
    @transient val largeWindow = Window.partitionBy("sat").orderBy("time").rangeBetween(-LARGE_WINDOW_SIZE, LARGE_WINDOW_SIZE)
    @transient val smallWindow = Window.partitionBy("sat").orderBy("time").rangeBetween(-SMALL_WINDOW_SIZE, SMALL_WINDOW_SIZE)

    val freq = 1575.42e6
    val sigma = 0.001
    val cNoAvg = 42.0

    // language=SQL
    val range = sc.read.jdbc(
      jdbcUri,
      s"""
          |(
          |SELECT
          |  time,
          |  anyIf(adr, freq = 'L1') AS adr1,
          |  anyIf(adr, freq = 'L2') AS adr2,
          |  anyIf(psr, freq = 'L1') AS psr1,
          |  anyIf(psr, freq = 'L2') AS psr2,
          |  any(system),
          |  any(glofreq),
          |  sat
          |FROM
          |  rawdata.range
          |WHERE
          |  sat='$sat' AND d BETWEEN toDate($from/1000) AND toDate($to/1000) AND time BETWEEN $from AND $to
          |GROUP BY
          |  time,
          |  sat
          |HAVING
          |  has(groupArray(freq) AS f, 'L1')
          |  AND has(f, 'L2')
          |ORDER BY
          |  time ASC
          |)
        """.stripMargin,
      jdbcProps
    )

    val Ks = range
      .withColumn("rank", dense_rank.over(rankWindow))
      .where($"rank" <= K_SET_SIZE)
      .agg(avg(k($"adr1", $"adr2", $"f1", $"f2", $"psr1", $"psr2")).as("K"))

    val computed = range
      .withColumn("f1", f($"system", $"freq", $"glofreq"))
      .withColumn("f2", f($"system", $"freq", $"glofreq"))
      //.withColumn("K", lit(0))
      .join(Ks, "sat")
      .withColumn("min", min($"time".as[Long]).over(Window.partitionBy($"sat")))
      .withColumn("max", max($"time".as[Long]).over(Window.partitionBy($"sat")))
      .withColumn("nt", nt($"adr1", $"adr2", $"f1", $"f2", $"K", lit(sigma)))
      .withColumn("delta", avg($"nt" - avg($"nt").over(largeWindow)).over(smallWindow))
      .withColumn("deviation", stddev($"delta").over(largeWindow))
      .withColumn("fluctuation", fluctuation($"deviation", lit(freq)))
      .withColumn("rice", rice($"fluctuation"))
      .withColumn("s4", s4($"fluctuation"))
      .withColumn("error_rice", errorRice($"rice", lit(cNoAvg)))
      .withColumn("error_s4", errorS4($"s4", lit(cNoAvg)))
      .where($"time".between($"min" + lit(LARGE_WINDOW_SIZE * 2), $"max" - lit(LARGE_WINDOW_SIZE * 2)))
      .drop("f1", "f2", "adr1", "adr2", "psr1", "psr2", "min", "max")

    computed.write.mode("append").jdbc(jdbcUri, "computed.range", jdbcProps)
    spark.close()
  }

  def printHelp(): Unit = {
    val usagestr = """
    Usage: <progname> <from> <to> <sat>
    <from> - start of the time range, (uint in ms)
    <to>   - end of the time range, (uint ms)
    <sat>  - name of satellite (string)
    """
    System.out.println(usagestr)
  }
}
