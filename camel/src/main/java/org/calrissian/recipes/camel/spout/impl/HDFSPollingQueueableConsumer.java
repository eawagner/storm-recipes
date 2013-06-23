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
package org.calrissian.recipes.camel.spout.impl;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.calrissian.recipes.camel.spout.QueueableConsumer;

import java.io.ByteArrayOutputStream;

/**
 * Demonstrates how to fill out a QueueableConsumer to poll files in an HDFS directory and chunk up their contents
 * to be made available in a queue as tuples.
 */
public class HDFSPollingQueueableConsumer extends QueueableConsumer {

    private final String baseDirectory;
    private final String namenode;

    private boolean localMode = false;

    public HDFSPollingQueueableConsumer(String baseDirectory, String namenode) {
        this.baseDirectory = baseDirectory;
        this.namenode = namenode;
    }

    @Override
    public void configure() throws Exception {

        String uriString = "hdfs://" + namenode + baseDirectory;

        if(localMode)
            uriString += "?fileSystemType=LOCAL";

        from(uriString)
                .process(new Processor() {

                    @Override
                    public void process(Exchange exchange) throws Exception {

                        System.out.println(exchange.getIn().getBody());
                        ByteArrayOutputStream body =  (ByteArrayOutputStream)exchange.getIn().getBody();

                        // TODO: Will need to deal with newline splits between different ingested chunks
                        String fileContents = new String(body.toByteArray());

                        String[] splits = fileContents.split("\n");

                        for(int i = 0; i < splits.length; i++)
                            getQueue().add(splits[i]);

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
