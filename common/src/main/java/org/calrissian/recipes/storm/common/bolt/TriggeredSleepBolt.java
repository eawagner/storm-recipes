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

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import static java.lang.System.currentTimeMillis;

/**
 * Date: 10/13/12
 * Time: 6:28 PM
 */
public class TriggeredSleepBolt extends BaseRichBolt {

    private final String id;
    private final String sourceId;
    private transient Timer timer;

    private final long delay;

    private OutputCollector collector;

    public TriggeredSleepBolt(String sourceId, String id, long delay) {
        this.delay = delay;
        this.id = id;
        this.sourceId = sourceId;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, final OutputCollector collector) {

        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        if(input.getSourceStreamId().equals(sourceId)) {

            timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    collector.emit(id, new Values(currentTimeMillis()));
                }
            }, delay);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(id, new Fields("ts"));
    }
}
