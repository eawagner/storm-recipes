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
