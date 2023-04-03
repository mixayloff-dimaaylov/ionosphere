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

package com.infocom.examples.spark.data;

/**
 * Частота L1 и L2, принимаемая со спутника и необходимая для вычислений
 */

public enum SignalType {
    Unknown(0),
    L1CA(1),
    L2C(2),
    L2CA(3),
    L2P(4),
    L2P_codeless(5),
    L2Y(6),
    L5Q(7);

    private final int value;

    SignalType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
