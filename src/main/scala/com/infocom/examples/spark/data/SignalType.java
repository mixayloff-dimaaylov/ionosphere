package com.infocom.examples.spark.data;

/**
 * Частота L1 и L2, принимаемая со спутника и необходимая для вычислений
 */

public enum SignalType {
    Unknown(0),
    L1(1),
    L2(2),
    L5(3);

    private final int value;

    SignalType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
