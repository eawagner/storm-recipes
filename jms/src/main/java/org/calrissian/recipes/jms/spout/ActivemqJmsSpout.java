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
package org.calrissian.recipes.jms.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Map;

/**
 * Date: 10/12/12
 * Time: 12:11 PM
 */
public class ActivemqJmsSpout extends BaseRichSpout implements MessageListener {

    private final String url;
    private final String username;
    private final String password;
    private final String topic;
    private final Fields output;

    private transient ConnectionFactory connectionFactory;
    private transient Connection connection;
    private transient Session session;
    private MessageConsumer consumer;
    private SpoutOutputCollector collector;

    public ActivemqJmsSpout(String url, String username, String password, String topic, Fields output) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.topic = topic;
        this.output = output;
    }

    @Override
    public void close() {
        super.close();
        try {
            if (consumer != null) {
                consumer.close();
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
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(output);
    }


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        connectionFactory = new ActiveMQConnectionFactory(username, password, url);
        //TODO: Should use spring for connection caching
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            consumer = session.createConsumer(session.createTopic(topic));
            consumer.setMessageListener(this);
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
    }

    @Override
    public void onMessage(Message message) {
        try {
            if (message instanceof ObjectMessage)
                collector.emit(new Values(((ObjectMessage) message).getObject()));
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }
}
