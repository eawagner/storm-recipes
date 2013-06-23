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
package org.calrissian.recipes.storm.common.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Date: 10/13/12
 * Time: 6:28 PM
 */
public class TimerSpout extends BaseRichSpout {

    private final String id;
    private transient Timer timer;

    private final long delay;
    private final long duration;

    public TimerSpout(String id, long duration) {
        this.duration = duration;
        this.delay = duration;
        this.id = id;
    }
    public TimerSpout(String id, long duration, long delay) {
        this.duration = duration;
        this.delay = delay;
        this.id = id;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(id, new Fields("ts"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, final SpoutOutputCollector spoutOutputCollector) {
        timer = new Timer();
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                spoutOutputCollector.emit(id, new Values(System.currentTimeMillis()));
            }
        }, delay, duration);
    }

    @Override
    public void nextTuple() {
    }

    @Override
    public void close() {
        super.close();
        timer.cancel();
    }
}
