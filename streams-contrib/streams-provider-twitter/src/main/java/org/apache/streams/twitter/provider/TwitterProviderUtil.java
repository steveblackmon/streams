package org.apache.streams.twitter.provider;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.streams.core.DatumStatus;
import org.apache.streams.core.StreamsDatum;
import org.apache.streams.twitter.TwitterConfiguration;
import org.apache.streams.util.ComponentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by sblackmon on 7/26/15.
 */
public class TwitterProviderUtil {

    private final static Logger LOGGER = LoggerFactory.getLogger(TwitterStreamProvider.class);

    public static String baseUrl(TwitterConfiguration config) {

        String baseUrl = new StringBuilder()
                .append(config.getProtocol())
                .append("://")
                .append(config.getHost())
                .append(":")
                .append(config.getPort())
                .append("/")
                .append(config.getVersion())
                .append("/")
                .toString();

        return baseUrl;
    }

    public static void drainTo(BlockingQueue<StreamsDatum> source, Queue<StreamsDatum> drain, int maxItems) throws Exception {
        int count = 0;
        while(!source.isEmpty() && count <= maxItems) {
            Queues.drain(source, drain, maxItems, 1, TimeUnit.SECONDS);
        }
    }

}
