/*
 * Copyright 2023 mixayloff-dimaaylov at github dot com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.infocom.examples.spark

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.mutable

import Functions._

object SigNtFunctions extends Serializable {
  /**
   * СКО флуктуаций фазы на фазовом экране
   *
   */
  def sigPhi(sigNT: Double, f: Double): Double = {
    1e16 * 80.8 * math.Pi * sigNT / (C * f)
  }

  /**
   * Расчет параметра Райса (глубины общих замираний)
   *
   */
  def gamma(sigPhi: Double): Double = {
    1 / math.exp(math.pow(sigPhi, 2) + 1)
  }

  /**
   * Расчет интервала частотной корреляции
   *
   */
  def fc(sigPhi: Double, f: Double): Double = {
    f / (math.sqrt(2) * sigPhi)
  }

  /**
   * Расчет интервала пространственной корреляции
   *
   */
  def pc(sigPhi: Double): Double = {
    val Lc = 200 //Средний размер неоднородностей
    Lc / sigPhi
  }
}

object TecCalculation extends Serializable {
  @transient var jdbcUri = ""
  @transient val jdbcProps = new Properties()
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  @transient private var DNTMap = mutable.Map[(String, String), Double]()

  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  @transient private val avgNTMap = mutable.Map[(String, String), DigitalFilter]()
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  @transient private val delNTMap = mutable.Map[(String, String), DigitalFilter]()

  {
    jdbcProps.setProperty("isolationLevel", "NONE")
    jdbcProps.setProperty("numPartitions", "1")
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.out.println("Wrong arguments")
      printHelp()
      System.exit(1)
    }

    if (args.length > 2) {
      System.out.println("Extra arguments")
      printHelp()
      System.exit(1)
    }

    val clickHouseServerAddress = args(0)
    jdbcUri = s"jdbc:clickhouse://$clickHouseServerAddress"

    fire(args(1))
  }

  private def mainJob(spark: SparkSession, from: Long): Long = {
    //runJobCorrelation(spark, from, to)
    runJob(spark, from)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var",
                          "org.wartremover.warts.While"))
  def fire(repeat: String): Unit = {
    val spark = getOrCreateSession("TEC Range Calculations")

    // "/ 1000 * 1000" - выравнивание по целым секундам. Для S4 и аналогичным.
    //Возьмем время минуту назад как начальная инициализация
    var from = new java.util.Date().getTime / 1000 * 1000 - 60000
    while (true) {
      from = mainJob(spark, from) + 1
      println(s"sleep $repeat ms")
      Thread.sleep(repeat.toLong)
    }

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

  def runJob(spark: SparkSession, from: Long): Long = {
    val sc = spark.sqlContext
    import spark.implicits._

    val range = sc.read.jdbc(
      jdbcUri,
      s"""
         |(
         |select
         |  sat,
         |  freq,
         |  ifNull(max(time), $from) as lseen
         |from
         |  rawdata.range
         |where
         |  d >= toDate($from/1000) AND time >= $from
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

    val to: Long =
      range
        .select($"lseen")
        .agg(min($"lseen"))
        .collect()
        .map((r: Row) => (r.getDecimal(0).longValue()))
        .toSeq(0)

    val rangeList =
      range
        .collect()
        .map((r: Row) => (r.getString(0), "L1CA+" + r.getString(1)))
        .toSeq

    println(s"from $from to $to (${to - from} ms) ")

    // выкинуть все значения, которых нет в range
    DNTMap --= (DNTMap -- rangeList).keys

    range.collect().foreach(row => {
      val sat = row(0).toString
      val f1Name = "L1CA"
      val f2Name = row(1).toString
      var sigComb = f1Name + "+" + f2Name

      // запустить функцию расчета для тех, что в range, если их нет
      if (!DNTMap.contains((sat, sigComb))) {
        val newDNTitem = ((sat, sigComb) -> calcDNT(spark, from, to, sat, f2Name))
        DNTMap += newDNTitem
      }

      sigComb = runJobNt(spark, from, to, sat, f2Name)
      runJobDerivatives(spark, from, to, sat, sigComb)
    })

    runJobXz1(spark, from, to)
    runJobS4cno(spark, from, to)
    runJobS4pwr(spark, from, to)
    runJobS4(spark, from, to)
    //runJobCorrelation(spark, from, to)

    to
  }

  def getDNT(sat: String, f1Name: String, f2Name: String): Double = {
    val sigcomb = s"$f1Name+$f2Name"
    DNTMap.get((sat, sigcomb)).getOrElse(0.0d)
  }

  def calcDNT(spark: SparkSession, from: Long, to: Long, sat: String, f2Name: String): Double = {
    val f1Name = "L1CA"
    val sigcomb = s"$f1Name+$f2Name"

    if (DNTMap.contains((sat, sigcomb))) {
      (DNTMap.get((sat, sigcomb))).getOrElse(0.0d)
    }

    println(s"DNT for $sat & $sigcomb")

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
         |  anyIf(freq, freq = '$f1Name') AS f1,
         |  anyIf(freq, freq = '$f2Name') AS f2,
         |  any(system) AS system,
         |  any(glofreq) AS glofreq,
         |  '$sigcomb' AS sigcomb,
         |  ifNull(any(sdcb), 0) AS sdcb
         |FROM
         |  rawdata.range
         |LEFT OUTER JOIN
         |  misc.sdcb
         |  ON (range.sat = sdcb.sat)
         |  AND (range.system = sdcb.system)
         |  AND (sigcomb = sdcb.sigcomb)
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

    val newDNT = range
      .withColumn("f1", f($"system", $"f1", $"glofreq"))
      .withColumn("f2", f($"system", $"f2", $"glofreq"))
      .select($"time", $"sat", $"adr1", $"adr2", $"f1", $"f2", $"psr1", $"psr2", $"sdcb")
      .groupBy($"sat")
      .agg(avg(k($"adr1", $"adr2", $"f1", $"f2", $"psr1", $"psr2", $"sdcb")).as("K"))
      .select("K")
      .map(r => r.getDouble(0))

    newDNT.collect().toSeq(0)
  }

  def runJobNt(spark: SparkSession, from: Long, to: Long, sat: String, f2Name: String): String = {
    val f1Name = "L1CA"
    val sigcomb = s"$f1Name+$f2Name"
    println(s"Nt for $sat & $sigcomb")

    val sc = spark.sqlContext
    import spark.implicits._

    val uGetDNT = udf(getDNT _)

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
         |  anyIf(freq, freq = '$f1Name') AS f1,
         |  anyIf(freq, freq = '$f2Name') AS f2,
         |  any(system) AS system,
         |  any(glofreq) AS glofreq,
         |  '$sigcomb' AS sigcomb,
         |  ifNull(any(sdcb), 0) AS sdcb
         |FROM
         |  rawdata.range
         |LEFT OUTER JOIN
         |  misc.sdcb
         |  ON (range.sat = sdcb.sat)
         |  AND (range.system = sdcb.system)
         |  AND (sigcomb = sdcb.sigcomb)
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
      .withColumn("DNT", uGetDNT($"sat", $"f1", $"f2"))
      .withColumn("f1", f($"system", $"f1", $"glofreq"))
      .withColumn("f2", f($"system", $"f2", $"glofreq"))

    //range.show()

    val tecRange = range
      .withColumn("nt", rawNt($"adr1", $"adr2", $"f1", $"f2", $"DNT"))
      .withColumn("adrNt", rawNt($"adr1", $"adr2", $"f1", $"f2", lit("0")))
      .withColumn("psrNt", psrNt($"psr1", $"psr2", $"f1", $"f2", lit("0")))
      .select("time", "sat", "sigcomb", "f1", "f2", "nt", "adrNt", "psrNt")

    //tecRange.show()

    //        CREATE TABLE computed.NT (
    //          time UInt64,
    //          sat String,
    //          sigcomb String,
    //          f1 Float64,
    //          f2 Float64,
    //          nt Float64,
    //          adrNt Float64,
    //          psrNt Float64,
    //          d Date MATERIALIZED toDate(round(time / 1000))
    //        ) ENGINE = ReplacingMergeTree(d, (time, sat, sigcomb), 8192)
    //        TTL d + INTERVAL 2 Week DELETE

    tecRange.write.mode("append").jdbc(jdbcUri, "computed.NT", jdbcProps)

    sigcomb
  }

  /*
   NOTE: S_4 is better to calculate according to the formula from Sigma_Phi in
   order to avoid the influence of multipath propagation

   May be *deprecated* in future.
   */
  def runJobS4cno(spark: SparkSession, from: Long, to: Long): Unit = {
    println(s"S_4 cno")

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

    //CREATE TABLE computed.s4cno (
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
      .write.mode("append").jdbc(jdbcUri, "computed.s4cno", jdbcProps)
  }

  /*
   NOTE: S_4 is better to calculate according to the formula from Sigma_Phi in
   order to avoid the influence of multipath propagation

   May be *deprecated* in future.
   */
  def runJobS4pwr(spark: SparkSession, from: Long, to: Long): Unit = {
    println(s"S_4 pwr")

    val sc = spark.sqlContext

    val result = sc.read.jdbc(
      jdbcUri,
      s"""
         |(
         |SELECT
         |  toUInt64(floor(time/1000,0)*1000) as time1s,
         |  sat,
         |  freq,
         |  sqrt((avg(pow(power,2)) - pow(avg(power),2)) / pow(avg(power),2)) as S4
         |FROM
         |  rawdata.ismdetobs
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

    //CREATE TABLE computed.s4pwr (
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
      .write.mode("append").jdbc(jdbcUri, "computed.s4pwr", jdbcProps)
  }

  def runJobS4(spark: SparkSession, from: Long, to: Long): Unit = {
    println(s"S_4 sigPhi")

    val sc = spark.sqlContext

    val result = sc.read.jdbc(
      jdbcUri,
      s"""
         |(
         |SELECT
         |  time,
         |  sat,
         |  sigcomb,
         |  sqrt(1 - exp(-2 * pow(any(sigPhi), 2))) as S4
         |FROM
         |  computed.xz1
         |WHERE
         |  d BETWEEN toDate($from/1000) AND toDate($to/1000) AND time BETWEEN $from AND $to
         |GROUP BY
         |  time,
         |  sat,
         |  sigcomb
         |)
        """.stripMargin,
      jdbcProps
    )

    //CREATE TABLE computed.s4 (
    //  time UInt64,
    //  sat String,
    //  sigcomb String,
    //  s4 Float64,
    //  d Date MATERIALIZED toDate(round(time / 1000))
    //) ENGINE = ReplacingMergeTree(d, (time, sat, freq), 8192)
    //TTL d + INTERVAL 2 Week DELETE

    result
      .select("time", "sat", "sigcomb", "s4")
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

    @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
    var time = rawData.select("time").map(r => r.getDecimal(0)).collect().toSeq
    val nt = rawData.select("nt").map(r => r.getDouble(0)).collect().toSeq
    var avgF = avgNTMap.getOrElse((sat, sigcomb), DigitalFilters.avgNt)
    var delF = delNTMap.getOrElse((sat, sigcomb), DigitalFilters.delNt)
    var avgNTSeq = avgF(nt)
    var delNtSeq = delF(nt)

    // TODO: Find a more mathematically correct solution to the problem of
    // spikes in general
    val df = (time, avgNTSeq, delNtSeq)
      .zipped.toSeq.toDF("time", "avgNT", "delNT")

    val result = rawData.join(df, Seq("time")).orderBy("time")

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

    avgNTMap((sat, sigcomb)) = avgF
    delNTMap((sat, sigcomb)) = delF
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

    val uSigPhi = udf(SigNtFunctions.sigPhi _)
    val uGamma = udf(SigNtFunctions.gamma _)
    val uFc = udf(SigNtFunctions.fc _)
    val uPc = udf(SigNtFunctions.pc _)

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

  def runJobCorrelation(spark: SparkSession, from: Long, to: Long): Unit = {
    println(s"Correlation")

    val sc = spark.sqlContext
    import spark.implicits._

    val rawData = sc.read.jdbc(
      jdbcUri,
      s"""
         |(
         |SELECT
         |  toUInt64(floor(time/10000,0)*10000)+10000 time,
         |  sat,
         |  sigcomb,
         |  delNT
         |FROM
         |	computed.NTDerivatives
         |WHERE
         |  d BETWEEN toDate($from/1000) AND toDate($to/1000) AND NTDerivatives.time BETWEEN $from AND $to
         |)
        """.stripMargin,
      jdbcProps
    )

    val uTimeCorrelation = udf(timeCorrelation _)
    //    val uTimeCorrelation0 = udf(timeCorrelationItem(0) _)
    //    val uTimeCorrelation1 = udf(timeCorrelationItem(1) _)
    //    val uTimeCorrelation2 = udf(timeCorrelationItem(2) _)
    //    val uTimeCorrelation3 = udf(timeCorrelationItem(3) _)
    //    val uTimeCorrelation4 = udf(timeCorrelationItem(4) _)
    //    val uTimeCorrelation5 = udf(timeCorrelationItem(5) _)
    //    val uTimeCorrelation6 = udf(timeCorrelationItem(6) _)
    //    val uTimeCorrelation7 = udf(timeCorrelationItem(7) _)

    val result = rawData
      .groupBy("time", "sat", "sigcomb")
      .agg(collect_list("delNT").as("delNTSeq"))
      .withColumn("Tc", uTimeCorrelation($"delNTSeq"))
    //      .withColumn("Tc0", uTimeCorrelation0($"delNTSeq"))
    //      .withColumn("Tc1", uTimeCorrelation1($"delNTSeq"))
    //      .withColumn("Tc2", uTimeCorrelation2($"delNTSeq"))
    //      .withColumn("Tc3", uTimeCorrelation3($"delNTSeq"))
    //      .withColumn("Tc4", uTimeCorrelation4($"delNTSeq"))
    //      .withColumn("Tc5", uTimeCorrelation5($"delNTSeq"))
    //      .withColumn("Tc6", uTimeCorrelation6($"delNTSeq"))
    //      .withColumn("Tc7", uTimeCorrelation7($"delNTSeq"))

    //result.show()

    //CREATE TABLE computed.Tc (
    //  time UInt64,
    //  sat String,
    //  sigcomb String,
    //  Tc Float64,
    //  d Date MATERIALIZED toDate(round(time / 1000))
    //) ENGINE = ReplacingMergeTree(d, (time, sat, sigcomb), 8192)
    //TTL d + INTERVAL 2 Week DELETE

    result
      .select("time", "sat", "sigcomb", "Tc")
      .write.mode("append").jdbc(jdbcUri, "computed.Tc", jdbcProps)
  }

  def printHelp(): Unit = {
    System.out.println(
      """
    Usage: <program name> <clickhouse_server> <milliseconds>
    <clickhouse_server>   - ClickHouse server (HTTP-interface) address:port, (string)
    <milliseconds> - milliseconds between calc, (integer)
    """
    )
  }
}

