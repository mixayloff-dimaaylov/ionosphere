package com.infocom.examples.spark.data;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class DataPointRange {
    public long Timestamp;
    public com.infocom.examples.spark.data.NavigationSystem NavigationSystem;
    public com.infocom.examples.spark.data.SignalType SignalType;
    public String Satellite;
    public int Prn;
    public int GloFreq ;
    public double Psr;
    public double Adr;
    public double CNo;
    public double LockTime;
    public double Power;

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
        tagsBuilder.put("freq", this.SignalType.name());

        return tagsBuilder.build();
    }
}
