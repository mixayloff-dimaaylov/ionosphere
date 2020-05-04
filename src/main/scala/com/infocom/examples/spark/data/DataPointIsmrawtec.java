package com.infocom.examples.spark.data;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Created by savartsov on 10/22/2016.
 */
public class DataPointIsmrawtec {
    public long Timestamp;
    public NavigationSystem NavigationSystem;
    public String Satellite;
    public int Prn;
    public int GloFreq;
    public SignalType PrimarySignal;
    public SignalType SecondarySignal;
    public double Tec;

    @Override
    public String toString() {
        return String.format("%s%d %d", NavigationSystem.name(), Prn, Timestamp);
    }

    public Map<String, String> toTags() {
        ImmutableMap.Builder<String, String> tagsBuilder = ImmutableMap.builder();

        tagsBuilder.put("sat", this.Satellite);
        tagsBuilder.put("prn", Integer.toString(this.Prn));
        tagsBuilder.put("system", this.NavigationSystem.name());
        tagsBuilder.put("glofreq", Integer.toString(this.GloFreq));
        tagsBuilder.put("primaryfreq", this.PrimarySignal.name());
        tagsBuilder.put("secondaryfreq", this.SecondarySignal.name());

        return tagsBuilder.build();
    }
}
