package org.calrissian.recipes.storm.common.mock;

import backtype.storm.spout.SpoutOutputCollector;

import java.util.List;

import static java.util.Collections.emptyList;


public class MockSpoutOutputCollector extends SpoutOutputCollector {

    protected List<Object> emittedTuples = emptyList();

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
