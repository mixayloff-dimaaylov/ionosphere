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

/*
 * Stateful digital filter with variable order.
 */

package com.infocom.examples.spark;

import scala.collection.mutable;

@SuppressWarnings(
  Array("org.wartremover.warts.Equals",
        "org.wartremover.warts.MutableDataStructures",
        "org.wartremover.warts.Throw"))
class DigitalFilter(
    val order: Int,
    val b: Seq[Double],
    val a: Seq[Double]) extends Serializable {

  if (order < 1) throw
    new IllegalArgumentException(s"The filter order must be positive number")

  if (b.length != (order + 1) || a.length != order) throw
    new IllegalArgumentException(s"The a's and b's lengths must be satisfy to filter order")

  private val zero: Double = 0
  private val zeroSeq = mutable.Buffer.fill[Double](order)(zero)

  var bInputSeq = zeroSeq
  var aInputSeq = zeroSeq

  private def filt(bInputSeq: Seq[Double], aInputSeq: Seq[Double]): Double = {
    if (b.length !== bInputSeq.length) throw
      new IllegalArgumentException(s"The length of b must be equal to bInputSeq length")

    if (a.length !== aInputSeq.length) throw
      new IllegalArgumentException(s"The length of a must be equal to aInputSeq length")

    ((b, bInputSeq).zipped.map((x, y) => x * y).sum
     - (a, aInputSeq).zipped.map((x, y) => x * y).sum)
  }

  def apply(input: Double): Double = {
    val iSeq = bInputSeq.padTo(order, zero) :+ input
    val oSeq = aInputSeq.padTo(order + 1, zero)

    for (i <- order until iSeq.length) {
      oSeq(i) =
        filt(
          iSeq.slice(i - order, i - 1 + 2).reverse,
          oSeq.slice(i - order, i - 1 + 1).reverse)
    }

    bInputSeq = iSeq.takeRight(order)
    aInputSeq = oSeq.takeRight(order)
    oSeq(0)
  }

  def apply(input: Seq[Double]): Seq[Double] = {
    val iSeq = bInputSeq.padTo(order, zero) ++ input
    val oSeq = aInputSeq.padTo(order + input.length, zero)

    for (i <- order until iSeq.length) {
      oSeq(i) =
        filt(
          iSeq.slice(i - order, i - 1 + 2).reverse,
          oSeq.slice(i - order, i - 1 + 1).reverse)
    }

    bInputSeq = iSeq.takeRight(order)
    aInputSeq = oSeq.takeRight(order)
    oSeq.slice(0, oSeq.length - order).toSeq
  }

  implicit final class AnyOps[A](self: A) {
    def !==(other: A): Boolean = self != other
  }
}
