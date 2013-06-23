package org.calrissian.recipes.storm.common.bolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.BoltTracker;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RegisteredGlobalState;
import com.google.common.hash.BloomFilter;
import org.calrissian.mango.domain.IPv4;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Date: 10/31/12
 * Time: 1:58 PM
 */
public class BloomFilterBoltTest {
    private static final Logger logger = LoggerFactory.getLogger(BloomFilterBoltTest.class);

    @Test
    public void testBloomFilterBolt() throws Exception {
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        String topologyName = "testTopo";
        TopologyBuilder builder = new TopologyBuilder();
        FeederSpout out = new FeederSpout(new Fields("ip"));
        builder.setSpout("feed_in", out);
        BloomFilter<IPv4> iPv4BloomFilter = BloomFilter.create(new IPv4Funnel(), 10);
        iPv4BloomFilter.put(new IPv4("1.1.1.3"));
        iPv4BloomFilter.put(new IPv4("1.1.1.5"));
        builder.setBolt("bloom_out", new BloomFilterBolt<IPv4>(iPv4BloomFilter)).shuffleGrouping("feed_in");
        builder.setBolt("print", new BoltTracker(new PrinterBolt(), topologyName)).shuffleGrouping("bloom_out");

        HashMap map = new HashMap();
        map.put("processed", new AtomicInteger(0));
        RegisteredGlobalState.setState(topologyName, map);

        cluster.submitTopology(topologyName, conf, builder.createTopology());

        Thread.sleep(2000);

        out.feed(new Values(new IPv4("1.1.1.1")));
        out.feed(new Values(new IPv4("1.1.1.2")));
        out.feed(new Values(new IPv4("1.1.1.3")));
        out.feed(new Values(new IPv4("1.1.1.4")));
        out.feed(new Values(new IPv4("1.1.1.5")));

        Thread.sleep(2000);

        cluster.shutdown();

        assertGlobalCount(topologyName, 2);
    }

    @Test
    public void testBloomFilterBoltWithUpdate() throws Exception {
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        String topologyName = "testTopo";
        TopologyBuilder builder = new TopologyBuilder();
        FeederSpout out = new FeederSpout(new Fields("ip"));
        builder.setSpout("feed_in", out);

        FeederSpout updateBloom = new FeederSpout(new Fields("bloom"));
        builder.setSpout("update_bloom", updateBloom);

        BloomFilter<IPv4> iPv4BloomFilter = BloomFilter.create(new IPv4Funnel(), 10);
        iPv4BloomFilter.put(new IPv4("1.1.1.3"));
        iPv4BloomFilter.put(new IPv4("1.1.1.5"));
        builder.setBolt("bloom_out", new BloomFilterBolt<IPv4>(iPv4BloomFilter, "update_bloom")).
                shuffleGrouping("feed_in").allGrouping("update_bloom");
        builder.setBolt("print", new BoltTracker(new PrinterBolt(), topologyName)).shuffleGrouping("bloom_out");

        HashMap map = new HashMap();
        map.put("processed", new AtomicInteger(0));
        RegisteredGlobalState.setState(topologyName, map);

        cluster.submitTopology(topologyName, conf, builder.createTopology());

        Thread.sleep(2000);

        out.feed(new Values(new IPv4("1.1.1.1")));
        out.feed(new Values(new IPv4("1.1.1.2")));
        out.feed(new Values(new IPv4("1.1.1.3")));
        out.feed(new Values(new IPv4("1.1.1.4")));
        out.feed(new Values(new IPv4("1.1.1.5")));

        Thread.sleep(2000);

        assertGlobalCount(topologyName, 2);

        iPv4BloomFilter = BloomFilter.create(new IPv4Funnel(), 10);
        iPv4BloomFilter.put(new IPv4("1.1.1.2"));
        iPv4BloomFilter.put(new IPv4("1.1.1.3"));
        iPv4BloomFilter.put(new IPv4("1.1.1.5"));
        updateBloom.feed(new Values(iPv4BloomFilter));

        Thread.sleep(2000);

        out.feed(new Values(new IPv4("1.1.1.1")));
        out.feed(new Values(new IPv4("1.1.1.2")));
        out.feed(new Values(new IPv4("1.1.1.3")));
        out.feed(new Values(new IPv4("1.1.1.4")));
        out.feed(new Values(new IPv4("1.1.1.5")));

        //now should hit three

        Thread.sleep(2000);

        assertGlobalCount(topologyName, 5);

        cluster.shutdown();
    }

    public static void assertGlobalCount(String topologyName, int expectedCount) {
        Map state = (Map) RegisteredGlobalState.getState(topologyName);
        assertNotNull(state);
        AtomicInteger processed = (AtomicInteger) state.get("processed");
        assertNotNull(processed);
        assertEquals(expectedCount, processed.intValue());
    }
}
