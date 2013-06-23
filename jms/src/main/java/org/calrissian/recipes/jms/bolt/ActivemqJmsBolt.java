package org.calrissian.recipes.jms.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.Serializable;
import java.util.Map;

/**
 * Date: 10/12/12
 * Time: 12:11 PM
 */
public class ActivemqJmsBolt extends BaseRichBolt {

    private final String url;
    private final String username;
    private final String password;
    private final String topic;

    private transient ConnectionFactory connectionFactory;
    private transient Connection connection;
    private transient Session session;
    private MessageProducer producer;

    public ActivemqJmsBolt(String url, String username, String password, String topic) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.topic = topic;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        connectionFactory = new ActiveMQConnectionFactory(username, password, url);
        //TODO: Should use spring for connection caching
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = session.createProducer(session.createTopic(topic));
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        try {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.stop();
                connection.close();
            }
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        //only sending object messages right now
        try {
            producer.send(session.createObjectMessage((Serializable) tuple.getValue(0)));
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //nothing, this is a sink
    }


}
