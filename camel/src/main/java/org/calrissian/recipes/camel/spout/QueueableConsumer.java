package org.calrissian.recipes.camel.spout;

import org.apache.camel.builder.RouteBuilder;

import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class QueueableConsumer extends RouteBuilder {

    protected ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<Object>();

    @Override
    public abstract void configure() throws Exception;

    public ConcurrentLinkedQueue<Object> getQueue() {
        return queue;
    }
}
