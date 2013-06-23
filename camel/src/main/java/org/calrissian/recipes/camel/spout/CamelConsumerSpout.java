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
