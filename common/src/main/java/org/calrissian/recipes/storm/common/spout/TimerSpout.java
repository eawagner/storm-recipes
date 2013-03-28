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

    private String id;
    private transient Timer timer;

    private long delay;
    private long duration;

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
