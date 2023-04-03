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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Created by kuzzm on 01.11.2016.
 */
public class DataPointIsmdetobs {
    public long Timestamp;
    public NavigationSystem NavigationSystem;
    public SignalType SignalType;
    public String Satellite;
    public int Prn;
    public int GloFreq;
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
