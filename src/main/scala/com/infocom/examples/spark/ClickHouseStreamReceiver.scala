package com.infocom.examples.spark

import java.util.UUID
import com.infocom.examples.spark.data._
import com.infocom.examples.spark.{StreamFunctions => SF}
import com.infocom.examples.spark.Functions._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object StreamReceiver {
  private def satGeoPoint: UserDefinedFunction = udf {
    (X: Double, Y: Double, Z: Double) => { SF.satGeoPoint(X, Y, Z) } : Long
  }

  private def satIonPoint: UserDefinedFunction = udf {
    (X: Double, Y: Double, Z: Double) => { SF.satIonPoint(X, Y, Z) } : Long
  }

  private def satElevation: UserDefinedFunction = udf {
    (X: Double, Y: Double, Z: Double) => { SF.satElevation(X, Y, Z) } : Double
  }

  private val schema = """
    |{
    |  "name": "NovAtelLogReader.DataPoints.DataPointSatxyz2",
    |  "type": "record",
    |  "fields": [
    |    {
    |      "name": "NavigationSystem",
    |      "type": {
    |        "name": "NovAtelLogReader.LogData.NavigationSystem",
    |        "type": "enum",
    |        "symbols": [
    |        "GPS",
    |        "GLONASS",
    |        "SBAS",
    |        "Galileo",
    |        "BeiDou",
    |        "QZSS",
    |        "Reserved",
    |        "Other"
    |        ]
    |      }
    |    },
    |    {
    |      "name": "Prn",
    |      "type": "int"
    |    },
    |    {
    |      "name": "Satellite",
    |      "type": "string"
    |    },
    |    {
    |      "name": "Timestamp",
    |      "type": "long"
    |    },
    |    {
    |      "name": "X",
    |      "type": "double"
    |    },
    |    {
    |      "name": "Y",
    |      "type": "double"
    |    },
    |    {
    |      "name": "Z",
    |      "type": "double"
    |    }
    |  ]
    |}""".stripMargin

  def main(args: Array[String]): Unit = {
    System.out.println("Run main")

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

    val kafkaServerAddress = args(0)
    val clickHouseServerAddress = args(1)
    val jdbcUri = s"jdbc:clickhouse://$clickHouseServerAddress"
    val clientUID = s"${UUID.randomUUID}"

    @transient val conf = new SparkConf().setAppName("GNSS Stream Receiver")
    val master = conf.getOption("spark.master")

    if (master.isEmpty) {
      conf.setMaster("local[*]")
    }

    System.out.println("Init conf")

    @transient val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    System.out.println("Create StreamingContext")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(60))
    System.out.println("Created StreamingContext")

    def createKafkaStream[TDataPoint](topic: String) = {
      val stream = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaServerAddress)
        .option("enable.auto.commit", (false: java.lang.Boolean))
        .option("auto.offset.reset", "latest")
        .option("group.id", s"gnss-stream-receiver-${clientUID}-${topic}")
        .option("subscribe", topic)
        .load()

      System.out.println($"Create $topic reader")

      stream
    }

    // RANGE

    createKafkaStream[DataPointRange]("datapoint-raw-range")
      .writeStream
      .option("checkpointLocation", "/tmp/infocom/datapont-raw-range")
      .outputMode("append")
      .format("streaming-jdbc")
      .option("url", jdbcUri)
      .option("table", "rawdata.range")
      .option("isolationLevel", "NONE")
      .start()

    // ISMREDOBS

    createKafkaStream[DataPointIsmredobs]("datapoint-raw-ismredobs")
      .writeStream
      .option("checkpointLocation", "/tmp/infocom/datapoint-raw-ismredobs")
      .outputMode("append")
      .format("streaming-jdbc")
      .option("url", jdbcUri)
      .option("table", "rawdata.ismredobs")
      .option("isolationLevel", "NONE")
      .start()

    // ISMDETOBS

    createKafkaStream[DataPointIsmdetobs]("datapoint-raw-ismdetobs")
      .writeStream
      .option("checkpointLocation", "/tmp/infocom/datapoint-raw-ismdetobs")
      .outputMode("append")
      .format("streaming-jdbc")
      .option("url", jdbcUri)
      .option("table", "rawdata.ismdetobs")
      .option("isolationLevel", "NONE")
      .start()

    // ISMRAWTEC

    createKafkaStream[DataPointIsmrawtec]("datapoint-raw-ismrawtec")
      .writeStream
      .option("checkpointLocation", "/tmp/infocom/datapoint-raw-ismrawtec")
      .outputMode("append")
      .format("streaming-jdbc")
      .option("url", jdbcUri)
      .option("table", "rawdata.ismrawtec")
      .option("isolationLevel", "NONE")
      .start()

    // SATXYZ2

    val win = Window.partitionBy("sat").orderBy("time")

    val prepared = createKafkaStream[DataPointSatxyz2]("datapoint-raw-satxyz2")
      .select(
        from_avro($"value", schema).as("point")
      )
      .select(
        $"point.Timestamp".as("time"),
        $"point.X".as("X"),
        $"point.Y".as("Y"),
        $"point.Z".as("Z"),
        $"point.Satellite".as("sat"),
        $"point.NavigationSystem".as("system"),
        $"point.Prn".as("prn")
      )
      // interpolate 1/10 Hz points to 50 Hz (20 ms step)
      // Previous lag
      .withColumn("timePrev",
        when(row_number.over(win) === 1, $"time").
        otherwise(lag($"time", 1).over(win))
      )
      .withColumn("XPrev",
        when(row_number.over(win) === 1, $"X").
        otherwise(lag($"X", 1).over(win))
      )
      .withColumn("YPrev",
        when(row_number.over(win) === 1, $"Y").
        otherwise(lag($"Y", 1).over(win))
      )
      .withColumn("ZPrev",
        when(row_number.over(win) === 1, $"Z").
        otherwise(lag($"Z", 1).over(win))
      )

    val interpolated = prepared.
      // Interpolate over each column
      withColumn("interpolatedList",
        tsInterpolate(20)($"timePrev", $"time",
          $"XPrev", $"X", $"YPrev", $"Y", $"ZPrev", $"Z")
      ).
      withColumn("interpolated", explode($"interpolatedList")).
      select(
        $"interpolated._1".as("time"),
        $"interpolated._2".as("X"),
        $"interpolated._3".as("Y"),
        $"interpolated._4".as("Z"),
        $"sat", $"system", $"prn"
      )

    val resulting = interpolated.
      withColumn("geopoint", satGeoPoint($"X", $"Y", $"Z")).
      withColumn("ionpoint", satIonPoint($"X", $"Y", $"Z")).
      withColumn("elevation", satElevation($"X", $"Y", $"Z")).
      select($"time", $"geopoint", $"ionpoint", $"elevation", $"sat", $"system", $"prn").
      writeStream.
      option("checkpointLocation", "/tmp/infocom/datapoint-raw-satxyz2").
      outputMode("append").
      format("streaming-jdbc").
      option("url", jdbcUri).
      option("table", "rawdata.satxyz2").
      option("isolationLevel", "NONE").
      start()

    System.out.println("Start StreamingContext")
  }

  def printHelp(): Unit = {
    val usagestr = """
    Usage: <progname> <kafka_server> <clickhouse_server>
    <kafka_server>        - Kafka server address:port, (string)
    <clickhouse_server>   - ClickHouse server (HTTP-interface) address:port, (string)
    """
    System.out.println(usagestr)
  }
}
