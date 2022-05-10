package com.infocom.examples.spark

import ch.hsr.geohash.GeoHash
import com.infocom.examples.spark.data.DataPointSatxyz2

/**
 * Created by savartsov on 02.05.2017.
 */
object StreamFunctions {
  // WGS84 ellipsoid constants
  private val observationLat: Double = 45.0409515
  private val observationLon: Double = 41.9108996
  private val observationAlt: Double = 652.1387
  private val a: Double = 6378137 // radius
  private val e: Double = 8.1819190842622e-2 // eccentricity
  private val asq: Double = Math.pow(a, 2)
  private val esq: Double = Math.pow(e, 2)

  private val axleA: Double = 6728137
  private val axleB: Double = 6706752.3142

  // Декартовы координаты приемника
  private val receiver: Array[Double] = lla2ecef(Math.toRadians(observationLat), Math.toRadians(observationLon), observationAlt)

  implicit class GeoHashExt(geoHash: GeoHash) {
    def longValueLeft: Long = {
      val shift = java.lang.Long.SIZE - geoHash.significantBits
      val mask = (1L << geoHash.significantBits) - 1

      (geoHash.longValue >> shift) & mask
    }
  }

  def get_vector(start: Array[Double], end: Array[Double]): Array[Double] = {
    Array(end(0) - start(0), end(1) - start(1), end(2) - start(2))
  }

  def get_vector_length(vector: Array[Double]): Double = {
    Math.sqrt(vector(0) * vector(0) + vector(1) * vector(1) + vector(2) * vector(2))
  }

  def get_unit_vector(vector: Array[Double]): Array[Double] = {
    val length = get_vector_length(vector)

    if (Math.abs(length - 1e-8) > 0.0) {
      Array(vector(0) / length, vector(1) / length, vector(2) / length)
    } else {
      vector
    }
  }

  /**
   * координаты точки пересечения луча и эллипсоида
   * координаты спутника, направляющий вектор, полуось a, полуось b
   */
  def intersection(S: Array[Double], n: Array[Double]): Array[Double] = {
    val x1: Double = S(0)
    val y1: Double = S(1)
    val z1: Double = S(2)
    val nx: Double = n(0)
    val ny: Double = n(1)
    val nz: Double = n(2)
    val a2: Double = axleA * axleA
    val b2: Double = axleB * axleB
    val A: Double = b2 * (nx * nx + ny * ny) + a2 * nz * nz
    val B: Double = 2 * nx * x1 * b2 + 2 * ny * y1 * b2 + 2 * nz * z1 * a2
    val C: Double = b2 * (x1 * x1 + y1 * y1 - a2) + a2 * z1 * z1
    val D: Double = B * B - 4 * A * C

    if (D < 0) {
      Array(0, 0, 0)
    } else {
      val t = (-B - Math.sqrt(D)) / (2 * A)
      Array(x1 + t * nx, y1 + t * ny, z1 + t * nz)
    }
  }

  /**
   * вектор нормали к поверхности эллипсоида в точке
   * R - точка на поверхности эллипсоида, а и b - полуоси
   */
  def normal(R: Array[Double]): Array[Double] = {
    Array(2 * R(0) / (axleA * axleA), 2 * R(1) / (axleA * axleA), 2 * R(2) / (axleB * axleB))
  }

  def cos_norm_RS(normal: Array[Double], RS: Array[Double]): Double = {
    val normal_length = get_vector_length(normal)
    val RS_length = get_vector_length(RS)

    if (Math.abs(normal_length - 1e-8) > 0.0 && Math.abs(RS_length - 1e-8) > 0.0) {
      (normal(0) * RS(0) + normal(1) * RS(1) + normal(2) * RS(2)) / normal_length / RS_length
    } else {
      0.0
    }
  }

  def getElevation(normalVector: Array[Double], recSatVector: Array[Double]): Double = {
    Math.toDegrees(Math.PI / 2 - Math.acos(cos_norm_RS(normalVector, recSatVector)))
  }

  def lla2ecef(lat: Double, lon: Double, alt: Double): Array[Double] = {
    val N: Double = a / Math.sqrt(1 - esq * Math.pow(Math.sin(lat), 2))

    val x: Double = (N + alt) * Math.cos(lat) * Math.cos(lon)
    val y: Double = (N + alt) * Math.cos(lat) * Math.sin(lon)
    val z: Double = ((1 - esq) * N + alt) * Math.sin(lat)

    Array(x, y, z)
  }

  def ecef2lla(ecef: Array[Double]): Array[Double] = {
    val x: Double = ecef(0)
    val y: Double = ecef(1)
    val z: Double = ecef(2)

    val b: Double = Math.sqrt(asq * (1 - esq))
    val bsq: Double = Math.pow(b, 2)
    val ep: Double = Math.sqrt((asq - bsq) / bsq)
    val p: Double = Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2))
    val th: Double = Math.atan2(a * z, b * p)

    val lon: Double = Math.atan2(y, x)
    val lat: Double = Math.atan2(z + Math.pow(ep, 2) * b * Math.pow(Math.sin(th), 3), p - esq * a * Math.pow(Math.cos(th), 3))
    val N: Double = a / Math.sqrt(1 - esq * Math.pow(Math.sin(lat), 2))
    val alt: Double = p / Math.cos(lat) - N

    // correction for altitude near poles left out
    // mod lat to 0-2pi
    Array(Math.toDegrees(lat), Math.toDegrees(lon % (2 * Math.PI)), alt)
  }

  @deprecated("Do not convert raw objects to GeoHash form", "logserver-spark 0.2.0")
  def satGeoPoint(xyz: DataPointSatxyz2): Long = {
    satGeoPointRaw(xyz.X, xyz.Y, xyz.Z)
  }

  def satGeoPointRaw(X: Double, Y: Double, Z: Double): Long = {
    val lla = ecef2lla(Array(X, Y, Z))

    GeoHash.withBitPrecision(lla(0), lla(1), 52).longValueLeft
  }

  @deprecated("Do not convert raw objects to GeoHash form", "logserver-spark 0.2.0")
  def satIonPoint(xyz: DataPointSatxyz2): Long = {
    satIonPointRaw(xyz.X, xyz.Y, xyz.Z)
  }

  def satIonPointRaw(X: Double, Y: Double, Z: Double): Long = {
    val point = Array(X, Y, Z)
    val lla = ecef2lla(intersection(point, get_unit_vector(get_vector(point, receiver))))

    GeoHash.withBitPrecision(lla(0), lla(1), 52).longValueLeft
  }

  @deprecated("Do not convert raw objects to GeoHash form", "logserver-spark 0.2.0")
  def satElevation(xyz: DataPointSatxyz2): Double = {
    satElevationRaw(xyz.X, xyz.Y, xyz.Z)
  }

  def satElevationRaw(X: Double, Y: Double, Z: Double): Double = {
    val point = Array(X, Y, Z)

    getElevation(normal(receiver), get_vector(receiver, point))
  }
}
