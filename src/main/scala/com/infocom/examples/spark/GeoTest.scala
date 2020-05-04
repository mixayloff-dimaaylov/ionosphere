package com.infocom.examples.spark

import ch.hsr.geohash.GeoHash

/**
 * Created by savartsov on 02.05.2017.
 */
object GeoTest {
  implicit class GeoHashExt(geoHash: GeoHash) {
    def longValueLeft: Long = {
      val shift = java.lang.Long.SIZE - geoHash.significantBits
      val mask = (1L << geoHash.significantBits) - 1
      (geoHash.longValue >> shift) & mask
    }
  }

  def main(args: Array[String]): Unit = {
    val hash = GeoHash.withBitPrecision(45.029180, 41.975252, 52)

    println(hash)
    println(hash.longValueLeft)
  }
}
