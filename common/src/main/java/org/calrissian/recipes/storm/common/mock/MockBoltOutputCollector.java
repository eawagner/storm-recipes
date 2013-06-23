package org.calrissian.recipes.storm.common.mock;

import backtype.storm.task.OutputCollector;

import java.util.List;

import static java.util.Collections.emptyList;

public class MockBoltOutputCollector extends OutputCollector {


    protected List<Object> emittedTuples = emptyList();

    public MockBoltOutputCollector() {
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
