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
 * Тип навигационной системы (GPS или Глонасс)
 */

public enum NavigationSystem {
    GPS(0),
    GLONASS(1),
    SBAS(2),
    Galileo(3),
    BeiDou(4),
    QZSS(5),
    Reserved(6),
    Other(7);

    private final int value;

    NavigationSystem(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
