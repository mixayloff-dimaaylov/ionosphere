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

  def f: UserDefinedFunction = udf {
    (system: String, freq: String, glofreq: Int) =>
      system match {
        case "GLONASS" =>
          freq match {
            case "L1CA"       => 1602.0e6 + glofreq * 0.5625e6
            case "L2CA"       => 1246.0e6 + glofreq * 0.4375e6
            case "L2P"        => 1246.0e6 + glofreq * 0.4375e6
            case _            => 0
          }

        case "GPS" =>
          freq match {
            case "L1CA"       => 1575.42e6
            case "L2C"        => 1227.60e6
            case "L2P"        => 1227.60e6
            case "L5Q"        => 1176.45e6
            case _            => 0
          }

        case _ => 0
      }
  }

  @deprecated("Duplicates functionality of f()", "logserver-spark 0.2.0")
  def f1: UserDefinedFunction = udf {
    (system: String, glofreq: Int) =>
      system match {
        case "GLONASS" => 1602.0e6 + glofreq * 0.5625e6
        case "GPS" => 1575.42e6
        case _ => 1575.42e6
      }
  }

  @deprecated("Duplicates functionality of f()", "logserver-spark 0.2.0")
  def f2: UserDefinedFunction = udf {
    (system: String, glofreq: Int) =>
      system match {
        case "GLONASS" => 1246.0e6 + glofreq * 0.4375e6
        case "GPS" => 1227.60e6
        case _ => 1227.60e6
      }
  }

  def waveLength(f: Double): Double = C / f

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  def tsInterpolate(dt: Long) = udf {
    (ts1: Long, ts2: Long, XPrev: Double, X: Double, YPrev: Double, Y: Double,
      ZPrev: Double, Z: Double) => {
      val perMinuteTS: IndexedSeq[Long]
        = if (ts1.equals(ts2)) Vector(ts1) else {
          val ldt1 = ts1
          val ldt2 = ts2
          Iterator.iterate(ldt1 + dt)(_ + dt).
              takeWhile(_ <= ldt2).
              toIndexedSeq
        }

      val perMinuteX: IndexedSeq[Double] = for {
        i <- 1 to perMinuteTS.size
      } yield XPrev + ((X - XPrev) * i / perMinuteTS.size)

      val perMinuteY: IndexedSeq[Double] = for {
        i <- 1 to perMinuteTS.size
      } yield YPrev + ((Y - YPrev) * i / perMinuteTS.size)

      val perMinuteZ: IndexedSeq[Double] = for {
        i <- 1 to perMinuteTS.size
      } yield ZPrev + ((Z - ZPrev) * i / perMinuteTS.size)

      val likeTuples
        = (perMinuteTS.toList
            zip perMinuteX.toList
            zip perMinuteY.toList
            zip perMinuteZ.toList)

      likeTuples.map({
        _ match {
          case (((a, b), c), d) => (a, b, c, d)
        }
      })
    }
  }

  def k: UserDefinedFunction = udf {
    (adr1: Double, adr2: Double, f1: Double, f2: Double, psr1: Double, psr2: Double, sdcb: Double)
      => (psr2 - psr1 + sdcb * C) - (adr2 * waveLength(f2) - adr1 * waveLength(f1))
  }

  def dnt: UserDefinedFunction = udf {
    (f1: Double, f2: Double, K: Double) =>
    {
      val f1_2 = f1 * f1
      val f2_2 = f2 * f2

      ((1e-16 * f1_2 * f2_2) / (40.308 * (f1_2 - f2_2))) * K
    }
  }

  /*
   * @param dnt смещение, м
   * @param sdcb поправка спутника, нс
   */
  def nt: UserDefinedFunction = udf {
    (adr1: Double, adr2: Double, f1: Double, f2: Double, dnt: Double, sdcb: Double) =>
      {
        val f1_2 = f1 * f1
        val f2_2 = f2 * f2

        ((1e-16 * f1_2 * f2_2) / (40.308 * (f1_2 - f2_2))) * (adr2 * waveLength(f2) - adr1 * waveLength(f1) + dnt + sdcb * C)
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
