package org.calrissian.stormrecipes.common.mock;

import backtype.storm.task.OutputCollector;

import java.util.ArrayList;
import java.util.List;

public class MockBoltOutputCollector extends OutputCollector {


    protected List<Object> emittedTuples = new ArrayList<Object>();

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
