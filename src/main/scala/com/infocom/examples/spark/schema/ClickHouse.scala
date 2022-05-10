package com.infocom.examples.spark.schema

import com.infocom.examples.spark.data._

object ClickHouse {
  /**
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
   * @param time
   * @param X
   * @param Y
   * @param Z
   * @param sat
   * @param system
   * @param prn
   */
  case class Satxyz2Row(time: Long, X: Double, Y: Double, Z: Double, sat: String, system: String, prn: Int)

  def toRow(dp: DataPointSatxyz2): Satxyz2Row = {
    System.out.println("Get DataPointSatxyz2")

    Satxyz2Row(
      dp.Timestamp,
      dp.X,
      dp.Y,
      dp.Z,
      dp.Satellite,
      dp.NavigationSystem.toString,
      dp.Prn
    )
  }
}
