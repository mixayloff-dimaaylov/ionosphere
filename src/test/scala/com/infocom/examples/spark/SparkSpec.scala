/*
 * Copyright 2016 CGnal S.p.A.
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

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{ AvroInputFormat, AvroWrapper }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{ BeforeAndAfterAll, MustMatchers, WordSpec }

@SuppressWarnings(Array("org.wartremover.warts.Overloading"))
final case class Person(name: String, age: Int)

class SparkSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  var sparkSession: SparkSession = _

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def beforeAll(): Unit = {
    val conf = new SparkConf().
      setAppName("spark-cdh5-template-local-test").
      setMaster("local[16]")
    sparkSession = SparkSession.builder().config(conf).getOrCreate()
    ()
  }

  /*
  "Spark" must {
    "load an avro file as a schema rdd correctly" in {

      val input = s"file://${System.getProperty("user.dir")}/src/test/resources/test.avro"

      import com.databricks.spark.avro._

      val data = sparkSession.read.avro(input)

      data.createOrReplaceTempView("test")

      val res = sparkSession.sql("select * from test where a < 10")

      res.collect().toList.toString must
        be("List([0,CIAO0], [1,CIAO1], [2,CIAO2], [3,CIAO3], [4,CIAO4], [5,CIAO5], [6,CIAO6], [7,CIAO7], [8,CIAO8], [9,CIAO9])")
    }
  }

  "Spark" must {
    "load an avro file as generic records" in {

      val input = s"file://${System.getProperty("user.dir")}/src/test/resources/test.avro"

      val rdd = sparkSession.sparkContext.hadoopFile[AvroWrapper[GenericRecord], NullWritable](
        input,
        classOf[AvroInputFormat[GenericRecord]],
        classOf[AvroWrapper[GenericRecord]],
        classOf[NullWritable]
      )
      @SuppressWarnings(Array("org.wartremover.warts.ToString"))
      val rows = rdd.map(gr => gr._1.datum().get("b").toString)

      rows.first() must be("CIAO0")
    }
  }

  "Spark" must {
    "save an schema rdd as an avro file correctly" in {

      val spark = sparkSession

      import com.databricks.spark.avro._
      import spark.implicits._

      val output = s"file://${System.getProperty("user.dir")}/tmp/test.avro"

      //I delete the output in case it exists
      val conf = new Configuration()
      val dir = new Path(output)
      val fileSystem = dir.getFileSystem(conf)
      if (fileSystem.exists(dir)) {
        val _ = fileSystem.delete(dir, true)
      }
      val peopleList: List[Person] = List(Person("David", 50), Person("Ruben", 14), Person("Giuditta", 12), Person("Vita", 19))
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      val people = sparkSession.sparkContext.parallelize[Person](peopleList).toDF()
      people.createOrReplaceTempView("people")

      val teenagers = sparkSession.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
      teenagers.write.avro(output)
      //Now I reload the file to check if everything is fine
      val data = sparkSession.read.avro(output)
      data.createOrReplaceTempView("teenagers")
      sparkSession.sql("select * from teenagers").collect().toList.toString must be("List([Ruben,14], [Vita,19])")
    }
  }
  */

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def afterAll(): Unit = {
    sparkSession.stop()
  }

}
