package com.infocom.examples.spark

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//noinspection ScalaStyle
object TecDerivativesCalculation {
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
         |SELECT
         |	sat,
         |	sigcomb
         |FROM
         |	computed.NT
         |where
         |  d BETWEEN toDate($from/1000) AND toDate($to/1000) AND time BETWEEN $from AND $to
         |group by
         |  sat,
         |  sigcomb
         |order by
         |  sat
         |)
        """.stripMargin,
      jdbcProps
    )

    range.collect().foreach(row => runJobDerivatives(spark, from, to, row(0).toString, row(1).toString))
  }

  def runJobDerivatives(spark: SparkSession, from: Long, to: Long, sat: String, sigcomb: String): Unit = {
    println(s"Derivatives for $sat & $sigcomb")

    val sc = spark.sqlContext
    import spark.implicits._

    val jdbcUri = s"jdbc:clickhouse://st9-ape-ionosphere2s-1:8123"
    @transient val jdbcProps = new Properties()
    jdbcProps.setProperty("isolationLevel", "NONE")

    val rawData = sc.read.jdbc(
      jdbcUri,
      s"""
         |(
         |SELECT
         |	time,
         |	sat,
         |	sigcomb,
         |	nt
         |FROM
         |	computed.NT
         |WHERE
         |  sat='$sat' AND d BETWEEN toDate($from/1000) AND toDate($to/1000) AND time BETWEEN $from AND $to
         |  and sigcomb='$sigcomb'
         |ORDER BY
         |  time
         |)
        """.stripMargin,
      jdbcProps
    )

    val time = rawData.select("time").map(r => r.getDecimal(0)).collect().toSeq
    val nt = rawData.select("nt").map(r => r.getDouble(0)).collect().toSeq

    val avgNTSeq = scala.collection.mutable.Seq.fill[Double](nt.length)(0)
    val delNtSeq = scala.collection.mutable.Seq.fill[Double](nt.length)(0)
    for(i <- 6 until nt.length)
    {
      val nt7 = Seq(nt(i), nt(i-1), nt(i-2), nt(i-3), nt(i-4), nt(i-5), nt(i-6))

      avgNTSeq(i) = avgNt(nt7, Seq(avgNTSeq(i-1), avgNTSeq(i-2), avgNTSeq(i-3), avgNTSeq(i-4), avgNTSeq(i-5), avgNTSeq(i-6)))
      delNtSeq(i) = delNt(nt7, Seq(delNtSeq(i-1), delNtSeq(i-2), delNtSeq(i-3), delNtSeq(i-4), delNtSeq(i-5), delNtSeq(i-6)))
    }

    val df = (time,avgNTSeq,delNtSeq).zipped.toSeq.toDF("time", "average", "fluctuation")
    //df.show

    val result = rawData.join(df, Seq("time")).orderBy("time")
    //result.show

    //CREATE TABLE computed.NTDerivatives (
    //  time UInt64,
    //  sat String,
    //  sigcomb String,
    //  average Float64,
    //  fluctuation Float64,
    //  d Date MATERIALIZED toDate(round(time / 1000))
    //) ENGINE = ReplacingMergeTree(d, (time, sat, sigcomb), 8192)

    result
      .select("time","sat","sigcomb","average","fluctuation")
      .write.mode("append").jdbc(jdbcUri, "computed.NTDerivatives", jdbcProps)
  }

  private def avgNt(nt: Seq[Double], avgNt: Seq[Double]): Double = {
    val b = Seq(
      0.00000004863987500780838,
      0.00000029183925004685027,
      0.00000072959812511712565,
      0.00000097279750015616753,
      0.00000072959812511712565,
      0.00000029183925004685027,
      0.00000004863987500780838
    )

    val a = Seq(
      -5.5145351211661655,
      12.689113056515138,
      -15.593635210704097,
      10.793296670485379,
      -3.9893594042308829,
      0.6151231220526282
    )

    ButterworthFilter(b, a, nt, avgNt)
  }

  private def delNt(nt: Seq[Double], delNt: Seq[Double]): Double = {
    val b = Seq(
      0.076745906902313671,
      0,
      -0.23023772070694101,
      0,
      0.23023772070694101,
      0,
      -0.076745906902313671
    )

    val a = Seq(
      -3.4767608600037727,
      5.0801848641096203,
      -4.2310052826910152,
      2.2392861745041328,
      -0.69437337677433475,
      0.084273573849621822
    )

    ButterworthFilter(b, a, nt, delNt)
  }

  private def ButterworthFilter(b: Seq[Double], a: Seq[Double], bInputSeq: Seq[Double], aInputSeq: Seq[Double]): Double = {
    if (b.length != bInputSeq.length) throw
      new IllegalArgumentException(s"The length of b must be equal to bInputSeq length")

    if (a.length != aInputSeq.length) throw
      new IllegalArgumentException(s"The length of a must be equal to aInputSeq length")

    (b, bInputSeq).zipped.map((x, y) => x * y).sum - (a, aInputSeq).zipped.map((x, y) => x * y).sum
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