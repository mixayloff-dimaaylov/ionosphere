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
   * @param sat Наименование спутника
   * @param system Навигационная система (GPS, GLONASS)
   * @param freq Наименование частоты (L1, L2, L5)
   * @param glofreq Частота GLONASS (-7..6)
   * @param prn Номер спутника
   */
  case class RangeRow(time: Long, adr: Double, psr: Double, cno: Double, sat: String, system: String, freq: String, glofreq: Int, prn: Int)

  def toRow(dp: DataPointRange): RangeRow = RangeRow(
    dp.Timestamp,
    dp.Adr,
    dp.Psr,
    dp.CNo,
    dp.Satellite,
    dp.NavigationSystem.toString,
    dp.SignalType.toString,
    dp.GloFreq,
    dp.Prn
  )

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

  def toRow(dp: DataPointSatxyz2): Satxyz2Row = Satxyz2Row(
    dp.Timestamp,
    StreamFunctions.satGeoPoint(dp),
    StreamFunctions.satIonPoint(dp),
    StreamFunctions.satElevation(dp),
    dp.Satellite,
    dp.NavigationSystem.toString,
    dp.Prn
  )
}
