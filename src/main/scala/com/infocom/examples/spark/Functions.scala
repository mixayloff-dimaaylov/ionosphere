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

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import breeze.integrate._
import breeze.numerics._
import scala.math
import scala.math.{Pi, sqrt}

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
            case "L1CA"         => 1575.42e6
            case "L2C"          => 1227.60e6
            case "L2P"          => 1227.60e6
            case "L2P_codeless" => 1227.60e6
            case "L5Q"          => 1176.45e6
            case _              => 0
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

  /**
   * ПЭС без поправок
   * @param dnt смещение, м
   */
  def psrNt: UserDefinedFunction = udf {
    (psr1: Double, psr2: Double, f1: Double, f2: Double, sdcb: Double) =>
      {
        val f1_2 = f1 * f1
        val f2_2 = f2 * f2

        ((1e-16 * f1_2 * f2_2) / (40.308 * (f1_2 - f2_2))) * (psr2 - psr1 + sdcb)
      }
  }

  /**
   * ПЭС без поправок
   * @param dnt смещение, м
   */
  def rawNt: UserDefinedFunction = udf {
    (adr1: Double, adr2: Double, f1: Double, f2: Double, dnt: Double) => {
      val f1_2 = f1 * f1
      val f2_2 = f2 * f2

      ((1e-16 * f1_2 * f2_2) / (40.308 * (f1_2 - f2_2))) * (adr2 * waveLength(f2) - adr1 * waveLength(f1) + dnt)
    }
  }

  /**
   * СКО флуктуаций фазы на фазовом экране
   *
   */
  def sigPhi: UserDefinedFunction = udf {
    (sigNT: Double, f: Double) => {
      1e16 * 80.8 * math.Pi * sigNT / (C * f)
    }
  }

  /**
   * Расчет параметра Райса (глубины общих замираний)
   *
   */
  def gamma: UserDefinedFunction = udf {
    (sigPhi: Double) => {
      1 / (math.exp(math.pow(sigPhi, 2)) - 1)
    }
  }

  /**
   * Расчет интервала частотной корреляции
   *
   */
  def fc: UserDefinedFunction = udf {
    (sigPhi: Double, f: Double) => {
      f / (math.sqrt(2) * sigPhi)
    }
  }

  /**
   * Расчет интервала пространственной корреляции
   *
   */
  def pc: UserDefinedFunction = udf {
    (sigPhi: Double) => {
      val Lc = 200 //Средний размер неоднородностей
      Lc / sigPhi
    }
  }

  /**
   * Расчет полосы дисперсионности
   *
   */
  def Fd: UserDefinedFunction = udf {
    (avgNT: Double, f0: Double) => {
      math.sqrt((C * math.pow(f0, 3)) / ((80.8 * math.Pi) * avgNT * 1e16))
    }
  }

  /**
   * Расчет полосы когерентности
   *
   */
  def Fk: UserDefinedFunction = udf {
    (sigPhi: Double, f0: Double) => {
      val Zm = 300000
      val l_s = 200
      (f0 /
        (sigPhi * math.sqrt(2 + ((4 * math.pow(Zm, 2) * (C * C))
          / ((math.Pi * math.Pi * math.pow(l_s, 4)) * (f0 * f0))))))
    }
  }

  /**
   * Расчет автокорреляционной функции (АКФ) флуктуаций ПЭС
   *
   * @param seq последовательность delNT
   * @return Интервал временной корреляции
   */
  def timeCorrelation(seq: Seq[Double]): Double = {
    val seqSum = (seq, seq).zipped.map(_ * _).sum

    val index = Seq.range(1, seq.length)
      .indexWhere(i => (seq.drop(i), seq).zipped.map(_ * _).sum / seqSum < 1 / Math.E)
      .toDouble

    if (index < 2) 0 else index * 0.02
  }

  /**
   * Рассчет i-го элемента АКФ флуктуаций ПЭС
   */
  def timeCorrelationItem(i: Int)(seq: Seq[Double]): Double = {
    val seqSum = (seq, seq).zipped.map(_ * _).sum

    (seq.drop(i), seq).zipped.map(_ * _).sum / seqSum
  }

  def fluctuation: UserDefinedFunction = udf {
    (deviation: Double, freq: Double) => (80.8 * Math.PI * deviation * 1e16) / (C * freq)
  }

  def s4: UserDefinedFunction = udf {
    (fluctuation: Double) => math.sqrt(1 - math.exp(-2.0 * fluctuation))
  }

  def s4intensity: UserDefinedFunction = udf {
    (a: Double, b: Double) => math.sqrt((a - b) / b)
  }

  def rice: UserDefinedFunction = udf {
    (fluctuation: Double) => 1 / math.exp(math.pow(fluctuation, 2) - 1)
  }

  def errorRice: UserDefinedFunction = udf {
    (rice: Double, cno: Double) =>
      ((rice + 1) / (cno + 2 * (rice + 1))) * math.exp(-(rice * cno) / (cno + 2 * (rice + 1)))
  }

  def errorS4: UserDefinedFunction = udf {
    (s4: Double, cno: Double) =>
      math.pow((2.0 / math.pow(s4, 2)) / (cno + (2 / math.pow(s4, 2))), 1 / math.pow(s4, 2)) / 2
  }

  /*
   * Функции расчета вероятности ошибки
   */
  def fresnel_C(z: Double): Double = {
    trapezoid((t) => math.cos(Pi * t * t / 2), 0, z, 1000)
  }

  def eta_m(_T_S: Double, _F_K: Double): Double = {
    val _v = 1.0 / (_T_S * _F_K)
    val _t = Pi * _T_S * _F_K

    (1.0 / (2 * Pi * Pi) * (_v * _v)
        * erf(_t)
      - 1.0 / (Pi * sqrt(Pi)) * _v * math.exp(-1.0 * (_t * _t)))
  }

  def eta_d(_F_0: Double, _F_d: Double): Double = {
    val _v = (_F_0 / _F_d)
    val _C_2 = fresnel_C(_v)
    (Pi * (_C_2 * _C_2)) / (2.0 * _v)
  }

  def eta_ch(_F_0: Double, _F_k: Double): Double = {
    val _v = Pi * _F_k / _F_0

    ((1.0 + (1 / 2 * Pi * Pi) * math.pow(_F_0 / _F_k, 2))
       * erf(_v)
       - 1.0 / (Pi * sqrt(Pi)) * (_F_0 / _F_k)
       * (2.0 - math.exp(-(_v * _v))))
  }

  def P_err(_h2s: Double, _gamma2: Double, _eta_ms: Double, _eta_chs: Double, _eta_ds: Double): Double = {
    val _g = _gamma2
    val _g_1 = _g + 1
    val _p = (_w: Double) => (_g_1) / (_w + 2.0 * _g_1) * math.exp(-1.0 * _g * _w / (_w + 2.0 * _g_1))

    val W111 = _h2s * _eta_ds * _eta_chs
    val W110 = (_h2s * _eta_ds * _eta_chs - _h2s * _eta_ds * _eta_ms) / (1.0 + _h2s * _eta_ds * _eta_ms)
    val W011 = W110
    val W010 = (_h2s * _eta_ds * _eta_chs - 2 * _h2s * _eta_ds * _eta_ms) / (1.0 + 2.0 * _h2s * _eta_ds * _eta_ms)

    val P111 = _p(W111)
    val P110 = _p(W110)
    val P011 = _p(W011)
    val P010 = _p(W010)

    0.25 * (P111 + P110 + P011 + P010)
  }

  def Perror: UserDefinedFunction = udf {
    (h2: Double, gamma2: Double, F_d: Double, F_k: Double) => {
      val R_T = 2.7 * 1e3
      val T_S = 1.0 / R_T
      val B_S = 1.0
      val F_0 = B_S / T_S

      P_err(h2, gamma2,
        eta_m(T_S, F_k),
        eta_ch(F_0, F_k),
        // TODO: fix eta_d
        1.0 /* eta_d(F_0, F_d) */)
    }
  }
}

object DNTEstimators extends Serializable {
  def regular(): DNTEstimator = {
    /* Timeout -- 1 minute, 3000 points if frequency of points = 50 Hz */
    new DNTEstimator(timeOut = 60000)
  }
}

object DigitalFilters extends Serializable {
  def avgNt(): DigitalFilter = {
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

    new DigitalFilter(6, b, a)
  }

  def delNt(): DigitalFilter = {
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

    new DigitalFilter(6, b, a)
  }
}
