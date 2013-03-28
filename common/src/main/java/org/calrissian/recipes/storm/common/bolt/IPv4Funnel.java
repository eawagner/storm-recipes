package org.calrissian.recipes.storm.common.bolt;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import org.calrissian.mango.types.types.IPv4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Date: 10/31/12
 * Time: 2:04 PM
 */
public class IPv4Funnel implements Funnel<IPv4>, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(IPv4Funnel.class);

    @Override
    public void funnel(IPv4 iPv4, PrimitiveSink primitiveSink) {
        primitiveSink.putLong(iPv4.getValue());
    }
}
