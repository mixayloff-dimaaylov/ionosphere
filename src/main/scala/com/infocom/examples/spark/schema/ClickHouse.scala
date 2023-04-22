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

package com.infocom.examples.spark.schema

import com.infocom.examples.spark.StreamFunctions
import com.infocom.examples.spark.data._

object ClickHouse {
  /**
   * CREATE TABLE rawdata.range (
   *   time UInt64,
   *   adr Float64,
   *   psr Float64,
   *   cno Float64,
   *   locktime Float64,
   *   sat String,
   *   system String,
   *   freq String,
   *   glofreq Int32,
   *   prn Int32,
   *   d Date MATERIALIZED toDate(round(time / 1000))
   * ) ENGINE = MergeTree(d, (time, sat, freq), 8192)
   *
   * @param time Время, мс
   * @param adr ADR
   * @param psr PSR
   * @param cno Сигнал/шум
   * @param locktime Время непрерывного слежения, c
   * @param sat Наименование спутника
   * @param system Навигационная система (GPS, GLONASS)
   * @param freq Наименование частоты (L1, L2, L5)
   * @param glofreq Частота GLONASS (-7..6)
   * @param prn Номер спутника
   */
  case class RangeRow(time: Long, adr: Double, psr: Double, cno: Double, locktime: Double, sat: String, system: String, freq: String, glofreq: Int, prn: Int)

  def toRow(dp: DataPointRange): RangeRow = {
    System.out.println("Get DataPointRange")
    RangeRow(
      dp.Timestamp,
      dp.Adr,
      dp.Psr,
      dp.CNo,
      dp.LockTime,
      dp.Satellite,
      dp.NavigationSystem.toString,
      dp.SignalType.toString,
      dp.GloFreq,
      dp.Prn
    )
  }

  /**
   * CREATE TABLE rawdata.ismredobs (
   *   time UInt64,
   *   totals4 Float64,
   *   sat String,
   *   system String,
   *   freq String,
   *   glofreq Int32,
   *   prn Int32,
   *   d Date MATERIALIZED toDate(round(time / 1000))
   * ) ENGINE = MergeTree(d, (time, sat, freq), 8192)
   *
   * @param time Время, мс
   * @param totals4 S4
   * @param sat Наименование спутника
   * @param system Навигационная система (GPS, GLONASS)
   * @param freq Наименование частоты (L1, L2, L5)
   * @param glofreq Частота GLONASS (-7..6)
   * @param prn Номер спутника
   */
  case class IsmredobsRow(time: Long, totals4: Double, sat: String, system: String, freq: String, glofreq: Int, prn: Int)

  def toRow(dp: DataPointIsmredobs): IsmredobsRow = IsmredobsRow(
    dp.Timestamp,
    dp.TotalS4,
    dp.Satellite,
    dp.NavigationSystem.toString,
    dp.SignalType.toString,
    dp.GloFreq,
    dp.Prn
  )

  /**
   * CREATE TABLE rawdata.ismdetobs (
   *   time UInt64,
   *   power Float64,
   *   sat String,
   *   system String,
   *   freq String,
   *   glofreq Int32,
   *   prn Int32,
   *   d Date MATERIALIZED toDate(round(time / 1000))
   * ) ENGINE = MergeTree(d, (time, sat, freq), 8192)
   *
   * @param time Время, мс
   * @param power S4
   * @param sat Наименование спутника
   * @param system Навигационная система (GPS, GLONASS)
   * @param freq Наименование частоты (L1, L2, L5)
   * @param glofreq Частота GLONASS (-7..6)
   * @param prn Номер спутника
   */
  case class IsmdetobsRow(time: Long, power: Double, sat: String, system: String, freq: String, glofreq: Int, prn: Int)

  def toRow(dp: DataPointIsmdetobs): IsmdetobsRow = IsmdetobsRow(
    dp.Timestamp,
    dp.Power,
    dp.Satellite,
    dp.NavigationSystem.toString,
    dp.SignalType.toString,
    dp.GloFreq,
    dp.Prn
  )

  /**
   * CREATE TABLE rawdata.ismrawtec (
   *   time UInt64,
   *   tec Float64,
   *   sat String,
   *   system String,
   *   primaryfreq String,
   *   secondaryfreq String,
   *   glofreq Int32,
   *   prn Int32,
   *   d Date MATERIALIZED toDate(round(time / 1000))
   * ) ENGINE = MergeTree(d, (time, sat, primaryfreq, secondaryfreq), 8192)
   *
   * @param time Время, мс
   * @param tec ПЭС
   * @param sat Наименование спутника
   * @param system Навигационная система (GPS, GLONASS)
   * @param primaryfreq Наименование первой частоты (L1, L2, L5)
   * @param secondaryfreq Наименование второй частоты (L1, L2, L5)
   * @param glofreq Частота GLONASS (-7..6)
   * @param prn Номер спутника
   */
  case class IsmrawtecRow(time: Long, tec: Double, sat: String, system: String, primaryfreq: String, secondaryfreq: String, glofreq: Int, prn: Int)

  def toRow(dp: DataPointIsmrawtec): IsmrawtecRow = IsmrawtecRow(
    dp.Timestamp,
    dp.Tec,
    dp.Satellite,
    dp.NavigationSystem.toString,
    dp.PrimarySignal.toString,
    dp.SecondarySignal.toString,
    dp.GloFreq,
    dp.Prn
  )

  /**
   * CREATE TABLE rawdata.satxyz2 (
   *   time UInt64,
   *   geopoint UInt64,
   *   ionpoint UInt64,
   *   elevation Float64,
   *   sat String,
   *   system String,
   *   prn Int32,
   *   d Date MATERIALIZED toDate(round(time / 1000))
   * ) ENGINE = MergeTree(d, (time, sat), 8192)
   *
   * @param time
   * @param geopoint
   * @param ionpoint
   * @param elevation
   * @param sat
   * @param system
   * @param prn
   */
  case class Satxyz2Row(time: Long, geopoint: Long, ionpoint: Long, elevation: Double, sat: String, system: String, prn: Int)

  def toRow(sf: StreamFunctions)(dp: DataPointSatxyz2): Satxyz2Row = {
    System.out.println("Get DataPointSatxyz2")

    Satxyz2Row(
      dp.Timestamp,
      sf.satGeoPoint(dp),
      sf.satIonPoint(dp),
      sf.satElevation(dp),
      dp.Satellite,
      dp.NavigationSystem.toString,
      dp.Prn
    )
  }
}
