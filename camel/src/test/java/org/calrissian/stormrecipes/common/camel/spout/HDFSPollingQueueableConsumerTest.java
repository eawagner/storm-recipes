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
package org.calrissian.stormrecipes.common.camel.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.base.BaseRichSpout;
import org.apache.commons.io.FileUtils;
import org.calrissian.recipes.camel.spout.CamelConsumerSpout;
import org.calrissian.recipes.camel.spout.impl.HDFSPollingQueueableConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class HDFSPollingQueueableConsumerTest {

    protected static String BASE_DIR = "target/test";

    @Before
    public void setUp() {
        new File(BASE_DIR).mkdir();
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File(BASE_DIR));
    }

    @Test
    public void testFileGetsImported() throws IOException, InterruptedException {

        SpoutOutputCollector collector = new MockSpoutOutputCollector();

        HDFSPollingQueueableConsumer routeBuilder = new HDFSPollingQueueableConsumer(new File(BASE_DIR).getAbsolutePath(), "localhost");
        routeBuilder.setLocalMode(true);

        BaseRichSpout spout = new CamelConsumerSpout(routeBuilder);
        spout.open(null, null, collector);

        BufferedWriter out = new BufferedWriter(new FileWriter(BASE_DIR + "/test.txt", false));
        out.write("Line1\nLine2");
        out.close();

        Thread.sleep(4000);

        spout.nextTuple();
        assertEquals("Line1", ((MockSpoutOutputCollector)collector).getEmittedTuples().get(0));
        spout.nextTuple();
        assertEquals("Line2", ((MockSpoutOutputCollector)collector).getEmittedTuples().get(0));
    }
}
