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
