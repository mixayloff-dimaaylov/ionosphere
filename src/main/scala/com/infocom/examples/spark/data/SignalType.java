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
