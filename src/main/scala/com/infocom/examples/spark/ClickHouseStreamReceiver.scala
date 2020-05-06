package com.infocom.examples.spark

import java.util.Properties
import com.infocom.examples.spark.data._
import com.infocom.examples.spark.schema.ClickHouse._
import com.infocom.examples.spark.serialization._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import scala.reflect._

object StreamReceiver {
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

    val kafkaServerAddress = args(0)
    val clickHouseServerAddress = args(1)
    val jdbcUri = s"jdbc:clickhouse://$clickHouseServerAddress"

    @transient val jdbcProps = new Properties()
    jdbcProps.setProperty("isolationLevel", "NONE")

    @transient val conf = new SparkConf().setAppName("GNSS Stream Receiver")
    val master = conf.getOption("spark.master")

    if (master.isEmpty) {
      conf.setMaster("local[*]")
    }

    @transient val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val ssc = new StreamingContext(spark.sparkContext, Seconds(60))

    def createKafkaStream[TDataPoint: ClassTag](topic: String): DStream[TDataPoint] = {
      val params = Map[String, Object](
        "bootstrap.servers" -> kafkaServerAddress,
        "key.deserializer" -> classOf[NullDeserializer],
        "value.deserializer" -> classOf[AvroDataPointDeserializer[Array[TDataPoint]]],
        "value.deserializer.type" -> classTag[Array[TDataPoint]].runtimeClass,
        "enable.auto.commit" -> (true: java.lang.Boolean),
        //"session.timeout.ms" -> "60000",
        "auto.offset.reset" -> "latest",
        "group.id" -> s"$topic-groupid"
      )

      val stream = KafkaUtils.createDirectStream[Null, Array[TDataPoint]](
        ssc,
        PreferConsistent,
        Subscribe[Null, Array[TDataPoint]](Seq(topic), params)
      ).asInstanceOf[InputDStream[ConsumerRecord[Null, Array[TDataPoint]]]]

      stream.start()
      stream.flatMap[TDataPoint](_.value())
    }

    // RANGE

    createKafkaStream[DataPointRange]("datapoint-raw-range") map toRow foreachRDD {
      _.toDF.write.mode("append").jdbc(jdbcUri, "rawdata.range", jdbcProps)
    }

    // SATXYZ2

    createKafkaStream[DataPointSatxyz2]("datapoint-raw-satxyz2") map toRow foreachRDD {
      _.toDF.write.mode("append").jdbc(jdbcUri, "rawdata.satxyz2", jdbcProps)
    }

    ssc.start()
    ssc.awaitTermination()
    spark.close()
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
