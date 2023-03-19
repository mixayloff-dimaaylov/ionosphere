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
 * Stateful DNT estimator.
 */

package com.infocom.examples.spark;

@SuppressWarnings(Array("org.wartremover.warts.Equals"))
class DNTEstimator(
  /*
   * Timeout to reset current DNT.
   */
    val timeOut: Long) extends Serializable {

  private var cnt: Int = 0
  private var acc: Double = 0
  private var lastSeen: Long = 0

  def reset() = {
    acc = 0
    cnt = 0
  }

  def apply(input: Double, time: Long): Double = {
    if((time - lastSeen) > timeOut) {
      reset()
    }

    if(cnt < 3000){
      acc += input
      cnt += 1
    }

    lastSeen = time
    acc / cnt
  }
}
