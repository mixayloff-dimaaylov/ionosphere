package com.infocom.examples.spark

import java.util.Properties

import Functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//noinspection ScalaStyle
object TecCalculation {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.out.println("Wrong arguments")
      printHelp()
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("TEC Range Calculations")
    val master = conf.getOption("spark.master")

    if (master.isEmpty) {
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder().config(conf).getOrCreate()
    runJob(spark, args(0).toLong, args(1).toLong)
    spark.close()
  }

  def runJob(spark: SparkSession, from: Long, to: Long): Unit = {
    val sc = spark.sqlContext

    val jdbcUri = s"jdbc:clickhouse://st9-ape-ionosphere2s-1:8123"
    @transient val jdbcProps = new Properties()
    jdbcProps.setProperty("isolationLevel", "NONE")

    val range = sc.read.jdbc(
      jdbcUri,
      s"""
         |(
         |select
         |  sat,
         |  freq
         |from
         |  rawdata.range
         |where
         |  d BETWEEN toDate($from/1000) AND toDate($to/1000) AND time BETWEEN $from AND $to
         |  and freq in('L2CA', 'L2C', 'L2P', 'L5Q')
         |group by
         |  sat,
         |  freq
         |order by
         |  sat
         |)
        """.stripMargin,
      jdbcProps
    )

    range.collect().foreach(row => runJobNt(spark, from, to, row(0).toString, row(1).toString))
  }

  def runJobNt(spark: SparkSession, from: Long, to: Long, sat: String, f2Name: String): Unit = {
    val f1Name = "L1CA"
    val sigcomb = s"$f1Name+$f2Name"
    println(s"Nt for $sat & $sigcomb")

    val sc = spark.sqlContext
    import spark.implicits._

    val jdbcUri = s"jdbc:clickhouse://st9-ape-ionosphere2s-1:8123"
    @transient val jdbcProps = new Properties()
    jdbcProps.setProperty("isolationLevel", "NONE")


    val range = sc.read.jdbc(
      jdbcUri,
      s"""
         |(
         |SELECT
         |  time,
         |  sat,
         |  anyIf(adr, freq = '$f1Name') AS adr1,
         |  anyIf(adr, freq = '$f2Name') AS adr2,
         |  anyIf(psr, freq = '$f1Name') AS psr1,
         |  anyIf(psr, freq = '$f2Name') AS psr2,
         |  any(system) AS system,
         |  any(glofreq) AS glofreq
         |FROM
         |  rawdata.range
         |WHERE
         |  sat='$sat' AND d BETWEEN toDate($from/1000) AND toDate($to/1000) AND time BETWEEN $from AND $to
         |  and freq in ('$f1Name', '$f2Name')
         |GROUP BY
         |  d,
         |  time,
         |  sat
         |HAVING
         |  has(groupArray(freq) AS f, '$f1Name')
         |  AND has(f, '$f2Name')
         |ORDER BY
         |  time ASC
         |)
        """.stripMargin,
      jdbcProps
    )
      .withColumn("f1", f(f1Name)($"system", $"glofreq"))
      .withColumn("f2", f(f2Name)($"system", $"glofreq"))

    //range.show()

    val kLimit = range
      .limit(K_SET_SIZE)
      .agg(avg(k($"adr1", $"adr2", $"f1", $"f2", $"psr1", $"psr2")).as("K"))

    //kLimit.show()

    val tecRange = range
      .crossJoin(kLimit)
      .withColumn("dnt", dnt($"f1", $"f2", $"K"))
      .withColumn("nt", nt($"adr1", $"adr2", $"f1", $"f2", $"dnt", lit(0)))
      .withColumn("sigcomb", lit(sigcomb))
      .select("time", "sat", "sigcomb", "nt")

    tecRange.show()

    tecRange.write.mode("append").jdbc(jdbcUri, "computed.NT", jdbcProps)
  }



  def printHelp(): Unit = {
    val usagestr =
      """
    Usage: <progname> <from> <to>
    <from> - start of the time range, (uint in ms)
    <to>   - end of the time range, (uint ms)
    """
    System.out.println(usagestr)
  }
}
