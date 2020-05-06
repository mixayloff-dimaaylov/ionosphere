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

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import scala.math._

object Functions extends Serializable {
  val LARGE_WINDOW_SIZE = 30000L
  val SMALL_WINDOW_SIZE = 40L
  val K_SET_SIZE = 3000

  val C = 299792458.0

  def toLong: UserDefinedFunction = udf {
    (time: Long) => time
  }

  def f1: UserDefinedFunction = udf {
    (system: String, glofreq: Int) =>
      system match {
        case "GLONASS" => 1602.0e6 + glofreq * 0.5625e6
        case "GPS" => 1575.42e6
        case _ => 1575.42e6
      }
  }

  def f2: UserDefinedFunction = udf {
    (system: String, glofreq: Int) =>
      system match {
        case "GLONASS" => 1246.0e6 + glofreq * 0.4375e6
        case "GPS" => 1227.60e6
        case _ => 1227.60e6
      }
  }

  def waveLength(f: Double): Double = C / f

  def k: UserDefinedFunction = udf {
    (adr1: Double, adr2: Double, f1: Double, f2: Double, psr1: Double, psr2: Double) => (adr1 * waveLength(f1) - adr2 * waveLength(f2)) - (psr1 - psr2)
  }

  def nt: UserDefinedFunction = udf {
    (l1: Double, l2: Double, f1: Double, f2: Double, k: Double, sigma: Double) =>
      {
        val f1_2 = f1 * f1
        val f2_2 = f2 * f2

        ((1e-16 * f1_2 * f2_2) / (40.308 * (f1_2 - f2_2))) * (l1 * waveLength(f1) - l2 * waveLength(f2) + k + sigma)
      }
  }

  def fluctuation: UserDefinedFunction = udf {
    (deviation: Double, freq: Double) => (80.8 * Math.PI * deviation * 1e16) / (C * freq)
  }

  def s4: UserDefinedFunction = udf {
    (fluctuation: Double) => sqrt(1 - exp(-2.0 * fluctuation))
  }

  def s4intensity: UserDefinedFunction = udf {
    (a: Double, b: Double) => math.sqrt((a - b) / b)
  }

  def rice: UserDefinedFunction = udf {
    (fluctuation: Double) => 1 / exp(pow(fluctuation, 2) - 1)
  }

  def errorRice: UserDefinedFunction = udf {
    (rice: Double, cno: Double) => ((rice + 1) / (cno + 2 * (rice + 1))) * exp(-(rice * cno) / (cno + 2 * (rice + 1)))
  }

  def errorS4: UserDefinedFunction = udf {
    (s4: Double, cno: Double) => pow((2.0 / pow(s4, 2)) / (cno + (2 / pow(s4, 2))), 1 / pow(s4, 2)) / 2
  }
}
