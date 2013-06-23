/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.recipes.storm.common.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.hash.BloomFilter;

import java.util.Map;

/**
 * Bolt holds a bloom filter, will output the objects that might be contained by the bloom filter
 * <p/>
 * Date: 10/31/12
 * Time: 1:52 PM
 */
public class BloomFilterBolt<T> extends BaseRichBolt {
    private BloomFilter<T> bloomFilter;
    private OutputCollector collector;
    private final String updateBloomFilterId;
    private boolean outputOnExistence = true; //If input exists in the BloomFilter, output

    public BloomFilterBolt(BloomFilter<T> bloomFilter) {
        this(bloomFilter, null);
    }

    public BloomFilterBolt(BloomFilter<T> bloomFilter, String updateBloomFilterId) {
        this(bloomFilter, updateBloomFilterId, true);
    }

    public BloomFilterBolt(BloomFilter<T> bloomFilter, String updateBloomFilterId, boolean outputOnExistence) {
        this.bloomFilter = bloomFilter;
        this.updateBloomFilterId = updateBloomFilterId;
        this.outputOnExistence = outputOnExistence;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (updateBloomFilterId != null) {
            if (updateBloomFilterId.equals(tuple.getSourceComponent())) {
                Object bloom = tuple.getValue(0);
                if (bloom instanceof BloomFilter) {
                    bloomFilter = (BloomFilter<T>) bloom;
                }
                return;
            }
        }
        if (bloomFilter == null) return; //no bloom filter, don't emit anything
        try {
            T value = (T) tuple.getValue(0);
            if (emitValue(value)) {
                collector.emit(new Values(value));
            }
        } catch (ClassCastException e) {
            //do nothing
        }
    }

    protected boolean emitValue(T value) {
        return outputOnExistence == bloomFilter.mightContain(value);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("obj"));
    }
}
