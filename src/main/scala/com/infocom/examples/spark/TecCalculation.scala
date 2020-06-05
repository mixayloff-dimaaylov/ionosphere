package com.infocom.examples.spark

import java.util.Properties

import com.infocom.examples.spark.Functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//noinspection ScalaStyle
object TecCalculation extends Serializable {
  @transient val jdbcUri = s"jdbc:clickhouse://st9-ape-ionosphere2s-1:8123"
  @transient val jdbcProps = new Properties()

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.out.println("Wrong arguments")
      printHelp()
      System.exit(1)
    }

    val now = (new java.util.Date).getTime
    val from = now - (args(0).toLong)

    jdbcProps.setProperty("isolationLevel", "NONE")
    jdbcProps.setProperty("numPartitions", "1")

    println(s"from $from to $now")

    val spark = getOrCreateSession("TEC Range Calculations")
    runJob(spark, from, now, args(1))
    spark.close()
  }

  private def getOrCreateSession(name: String): SparkSession = {
    val conf = new SparkConf().setAppName(name).set("spark.sql.allowMultipleContexts", "false")
    val master = conf.getOption("spark.master")

    if (master.isEmpty) {
      conf.setMaster("local[*]")
    }

    SparkSession.builder().config(conf).getOrCreate()
  }

  def runJob(spark: SparkSession, from: Long, to: Long, sat: String): Unit = {
    val sc = spark.sqlContext

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
//         |  and sat = '$sat'
         |group by
         |  sat,
         |  freq
         |order by
         |  sat
         |)
        """.stripMargin,
      jdbcProps
    )

    range.collect().foreach(row => {
      val sigComb = runJobNt(spark, from, to, row(0).toString, row(1).toString)
      runJobDerivatives(spark, from, to, row(0).toString, sigComb)
    })
    runJobXz1(spark, from, to)
    runJobS4(spark, from, to)
  }

  def runJobNt(spark: SparkSession, from: Long, to: Long, sat: String, f2Name: String): String = {
    val f1Name = "L1CA"
    val sigcomb = s"$f1Name+$f2Name"
    println(s"Nt for $sat & $sigcomb")

    val sc = spark.sqlContext
    import spark.implicits._

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
      .select("time", "sat", "sigcomb", "f1", "f2", "nt")

    //tecRange.show()

    //        CREATE TABLE computed.NT (
    //          time UInt64,
    //          sat String,
    //          sigcomb String,
    //          f1 Float64,
    //          f2 Float64,
    //          nt Float64,
    //          d Date MATERIALIZED toDate(round(time / 1000))
    //        ) ENGINE = ReplacingMergeTree(d, (time, sat, sigcomb), 8192)
    //        TTL d + INTERVAL 2 Week DELETE

    tecRange.write.mode("append").jdbc(jdbcUri, "computed.NT", jdbcProps)

    sigcomb
  }

  def runJobS4(spark: SparkSession, from: Long, to: Long): Unit = {
    println(s"S4")

    val sc = spark.sqlContext

    val result = sc.read.jdbc(
      jdbcUri,
      s"""
         |(
         |SELECT
         |  toUInt64(floor(time/1000,0)*1000) as time1s,
         |  sat,
         |  freq,
         |  sqrt((avg(pow(exp10(cno/10),2)) - pow(avg(exp10(cno/10)),2)) / pow(avg(exp10(cno/10)),2)) as S4
         |FROM
         |  rawdata.range
         |WHERE
         |  d BETWEEN toDate($from/1000) AND toDate($to/1000) AND time BETWEEN $from AND $to
         |GROUP BY
         |  floor(time/1000,0),
         |  sat,
         |  freq
         |)
        """.stripMargin,
      jdbcProps
    )

    //CREATE TABLE computed.s4 (
    //  time UInt64,
    //  sat String,
    //  freq String,
    //  s4 Float64,
    //  d Date MATERIALIZED toDate(round(time / 1000))
    //) ENGINE = ReplacingMergeTree(d, (time, sat, freq), 8192)
    //TTL d + INTERVAL 2 Week DELETE

    result
      .withColumnRenamed("time1s", "time")
      .select("time", "sat", "freq", "s4")
      .write.mode("append").jdbc(jdbcUri, "computed.s4", jdbcProps)
  }

  def runJobDerivatives(spark: SparkSession, from: Long, to: Long, sat: String, sigcomb: String): Unit = {
    println(s"Derivatives for $sat & $sigcomb")

    val sc = spark.sqlContext
    import spark.implicits._

    val rawData = sc.read.jdbc(
      jdbcUri,
      s"""
         |(
         |SELECT
         |	time,
         |	sat,
         |	sigcomb,
         |  f1,
         |  f2,
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
    for (i <- 6 until nt.length) {
      val nt7 = Seq(nt(i), nt(i - 1), nt(i - 2), nt(i - 3), nt(i - 4), nt(i - 5), nt(i - 6))

      avgNTSeq(i) = avgNt(nt7, Seq(avgNTSeq(i - 1), avgNTSeq(i - 2), avgNTSeq(i - 3), avgNTSeq(i - 4), avgNTSeq(i - 5), avgNTSeq(i - 6)))
      delNtSeq(i) = delNt(nt7, Seq(delNtSeq(i - 1), delNtSeq(i - 2), delNtSeq(i - 3), delNtSeq(i - 4), delNtSeq(i - 5), delNtSeq(i - 6)))
    }

    val df = (time, avgNTSeq, delNtSeq).zipped.toSeq.toDF("time", "avgNT", "delNT")
    //df.show

    val result = rawData.join(df, Seq("time")).orderBy("time")
    //result.show

    //        CREATE TABLE computed.NTDerivatives (
    //          time UInt64,
    //          sat String,
    //          sigcomb String,
    //          f1 Float64,
    //          f2 Float64,
    //          avgNT Float64,
    //          delNT Float64,
    //          d Date MATERIALIZED toDate(round(time / 1000))
    //        ) ENGINE = ReplacingMergeTree(d, (time, sat, sigcomb), 8192)
    //        TTL d + INTERVAL 2 Week DELETE

    result
      .select("time", "sat", "sigcomb", "f1", "f2", "avgNT", "delNT")
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

  def runJobXz1(spark: SparkSession, from: Long, to: Long): Unit = {
    println(s"Xz1")

    val sc = spark.sqlContext
    import spark.implicits._

    val rawData = sc.read.jdbc(
      jdbcUri,
      s"""
         |(
         |SELECT
         |  toUInt64(floor(time/1000,0)*1000) time,
         |  sat,
         |  sigcomb,
         |  any(f1) f1,
         |  any(f2) f2,
         |  sqrt(avg(pow(delNT,2))) sigNT
         |FROM
         |	computed.NTDerivatives
         |WHERE
         |  d BETWEEN toDate($from/1000) AND toDate($to/1000) AND time BETWEEN $from AND $to
         |GROUP BY
         |  floor(NTDerivatives.time/1000,0),
         |  sat,
         |  sigcomb
         |)
        """.stripMargin,
      jdbcProps
    )

    val uSigPhi = udf(sigPhi _)
    val uGamma = udf(gamma _)
    val uFc = udf(Fc _)
    val uPc = udf(Pc _)

    val result = rawData
      .withColumn("sigPhi", uSigPhi($"sigNT", $"f1"))
      .withColumn("gamma", uGamma($"sigPhi"))
      .withColumn("Fc", uFc($"sigPhi", $"f1"))
      .withColumn("Pc", uPc($"sigPhi"))

    //result.show

    //CREATE TABLE computed.xz1 (
    //  time UInt64,
    //  sat String,
    //  sigcomb String,
    //  f1 Float64,
    //  f2 Float64,
    //  sigNT Float64,
    //  sigPhi Float64,
    //  gamma Float64,
    //  Fc Float64,
    //  Pc Float64,
    //  d Date MATERIALIZED toDate(round(time / 1000))
    //) ENGINE = ReplacingMergeTree(d, (time, sat, sigcomb), 8192)
    //TTL d + INTERVAL 2 Week DELETE

    result
      .select("time", "sat", "sigcomb", "f1", "f2", "sigNT", "sigPhi", "gamma", "Fc", "Pc")
      .write.mode("append").jdbc(jdbcUri, "computed.xz1", jdbcProps)
  }

  /**
   * СКО флуктуаций фазы на фазовом экране
   *
   * @param sigNT
   * @param f
   * @return
   */
  def sigPhi(sigNT: Double, f: Double): Double = {
    10e16 * 80.8 * math.Pi * sigNT / (Functions.C * f)
  }

  /**
   * Расчет параметра Райса (глубины общих замираний)
   *
   * @param sigPhi
   * @return
   */
  def gamma(sigPhi: Double): Double = {
    1 / math.exp(math.pow(sigPhi, 2) + 1)
  }

  /**
   * Расчет интервала частотной корреляции
   *
   * @param sigPhi
   * @param f
   * @return
   */
  def Fc(sigPhi: Double, f: Double): Double = {
    f / (math.sqrt(2) * sigPhi)
  }

  /**
   * Расчет интервала пространственной корреляции
   *
   * @param sigPhi
   * @return
   */
  def Pc(sigPhi: Double): Double = {
    val Lc = 200 //Средний размер неоднородностей
    Lc / sigPhi
  }

  def printHelp(): Unit = {
    System.out.println(
      """
    Usage: <progname> <hour>
    <hour> - hour back to calc
    """)
  }
}
