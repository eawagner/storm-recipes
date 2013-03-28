package org.calrissian.stormrecipes.common.camel.spout;

import backtype.storm.spout.SpoutOutputCollector;

import java.util.ArrayList;
import java.util.List;

public class MockSpoutOutputCollector extends SpoutOutputCollector {

    protected List<Object> emittedTuples = new ArrayList<Object>();

    public MockSpoutOutputCollector() {
        super(null);
    }

    @Override
    public List<Integer> emit(List<Object> tuples) {

        this.emittedTuples = tuples;

        return null;
    }

    public List<Object> getEmittedTuples() {
        return emittedTuples;
    }
}
