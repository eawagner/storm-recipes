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

/**
 * Date: 10/13/12
 * Time: 6:28 PM
 */
public class TriggeredSleepBolt extends BaseRichBolt {

    private String id;
    private String sourceId;
    private transient Timer timer;

    private long delay;

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
                    collector.emit(id, new Values(System.currentTimeMillis()));
                }
            }, delay);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(id, new Fields("ts"));
    }
}
