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
package org.calrissian.recipes.camel.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;

import java.util.Map;

public class CamelConsumerSpout extends BaseRichSpout {

    private final QueueableConsumer routeBuilder;
    private transient CamelContext camelContext;

    private SpoutOutputCollector collector;

    public CamelConsumerSpout(QueueableConsumer routeBuilder) {
        this.routeBuilder = routeBuilder;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        this.collector = collector;

        this.camelContext = new DefaultCamelContext();

        try {
            camelContext.addRoutes(routeBuilder);
            camelContext.start();

        } catch (Exception e) {
        }
    }

    @Override
    public void nextTuple() {

        if(routeBuilder.getQueue().peek() != null) {
            collector.emit(new Values(routeBuilder.getQueue().remove()));
        }

        else {
            try {
                // It is recommended that we sleep so we aren't constantly cycling the CPU with no data
                Thread.sleep(5);
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void close() {
        try {
            camelContext.stop();
        } catch (Exception e) {
        }
        super.close();
    }
}
