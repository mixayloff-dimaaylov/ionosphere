package com.infocom.examples.spark.data;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Created by kuzzm on 15.07.2016.
 */
public class DataPointSatxyz2 {
    /** Временная отметка отпраки данных со спутника */
    public long Timestamp;

    /** Тип навигационной системы */
    public NavigationSystem NavigationSystem;

    /** Название спутника */
    public String Satellite;

    /** Номер спутника */
    public int Prn;

    public double X;
    public double Y;
    public double Z;

    public Map<String, String> toTags() {
        ImmutableMap.Builder<String, String> tagsBuilder = ImmutableMap.builder();

        tagsBuilder.put("sat", this.Satellite);
        tagsBuilder.put("prn", Integer.toString(this.Prn));
        tagsBuilder.put("system", this.NavigationSystem.name());

        return tagsBuilder.build();
    }
}
