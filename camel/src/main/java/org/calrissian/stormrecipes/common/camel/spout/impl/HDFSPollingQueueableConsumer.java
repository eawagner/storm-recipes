package org.calrissian.stormrecipes.common.camel.spout.impl;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.calrissian.stormrecipes.common.camel.spout.QueueableConsumer;

import java.io.ByteArrayOutputStream;

/**
 * Demonstrates how to fill out a QueueableConsumer to poll files in an HDFS directory and chunk up their contents
 * to be made available in a queue as tuples.
 */
public class HDFSPollingQueueableConsumer extends QueueableConsumer {

    String baseDirectory;
    String namenode;


    boolean localMode = false;

    public HDFSPollingQueueableConsumer(String baseDirectory, String namenode) {
        this.baseDirectory = baseDirectory;
        this.namenode = namenode;
    }

    @Override
    public void configure() throws Exception {

        String uriString = "hdfs://" + namenode + baseDirectory;

        if(localMode) {
            uriString += "?fileSystemType=LOCAL";
        }

        from(uriString)
                .process(new Processor() {

                    @Override
                    public void process(Exchange exchange) throws Exception {

                        System.out.println(exchange.getIn().getBody());
                        ByteArrayOutputStream body =  (ByteArrayOutputStream)exchange.getIn().getBody();

                        // TODO: Will need to deal with newline splits between different ingested chunks
                        String fileContents = new String(body.toByteArray());

                        String[] splits = fileContents.split("\n");

                        for(int i = 0; i < splits.length; i++) {
                            getQueue().add(splits[i]);
                        }
                    }
                });
    }

    public boolean isLocalMode() {
        return localMode;
    }

    public void setLocalMode(boolean localMode) {
        this.localMode = localMode;
    }
}
