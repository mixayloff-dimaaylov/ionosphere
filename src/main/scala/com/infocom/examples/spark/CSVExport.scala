package com.infocom.examples.spark

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.util.Try

object CSVExport {
  def remoteIteratorToScalaIterator[T](underlying: RemoteIterator[T]): Iterator[T] = {
    case class wrapper(underlying: RemoteIterator[T]) extends Iterator[T] {
      override def hasNext: Boolean = underlying.hasNext
      override def next: T = underlying.next
    }
    wrapper(underlying)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CSV Export")
    val master = conf.getOption("spark.master")

    if (master.isEmpty) {
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder().config(conf).getOrCreate()
    runJob(spark, args(0).toLong, args(1).toLong, args(2))
  }

  def runJob(spark: SparkSession, from: Long, to: Long, sat: String): Unit = {
    val sc = spark.sqlContext

    val jdbcUri = s"jdbc:clickhouse://st9"

    @transient val jdbcProps = new Properties()
    jdbcProps.setProperty("isolationLevel", "NONE")

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)

    val date = LocalDateTime.now()
    val prefix = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm-ss"))
    val dirName = s"/export/$prefix $sat"

    val readFrom = new Timestamp(from)
    val readTo = new Timestamp(to)
    val interval = s"${readFrom.getTime / 1000}:${readTo.getTime / 1000}"

    val tables = Seq(
      "rawdata.range",
      "rawdata.ismredobs",
      "rawdata.ismdetobs",
      "rawdata.ismrawtec",
      "rawdata.satxyz2"
    )

    for (t <- tables) {
      // language=SQL
      val df = sc.read.jdbc(
        jdbcUri,
        s"""(
          |SELECT
          | *
          |FROM
          | $t
          |WHERE
          | sat='$sat' AND d BETWEEN toDate($from/1000) AND toDate($to/1000) AND time BETWEEN $from AND $to
          |)""".stripMargin,
        jdbcProps
      )

      df.write.option("header", "true").csv(s"$dirName/$t")

      // Очистка пустых файлов
      Try {
        val files = hdfs.listFiles(new Path(s"$dirName/$t"), false)

        for (file <- remoteIteratorToScalaIterator(files)) {
          val name = file.getPath.getName

          if (name.startsWith("part") && file.getLen == 0) {
            hdfs.delete(file.getPath, false)
          }
        }
      }
    }

    Try {
      val out = hdfs.create(new Path(s"$dirName.zip"))
      HDFSArchiver.zip(hdfs, new Path(dirName), out)
      out.close()
    }

    spark.close()
  }
}

