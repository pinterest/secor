package com.pinterest.secor.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SingleProcessCounter {
    private static final Logger LOG = LoggerFactory.getLogger(SingleProcessCounter.class);

    private ConcurrentHashMap<TopicPartition, Long> mMessageUploadCounter;

    private ConcurrentHashMap<TopicPartition, Long> mMessageLocalCounter;

    private static volatile SingleProcessCounter counter = null;

    private static Object lock = new Object();

    private SingleProcessCounter() {
        mMessageLocalCounter = new ConcurrentHashMap<>();
        mMessageUploadCounter = new ConcurrentHashMap<>();
    }

    public static SingleProcessCounter getSingleProcessCounter() {
        if (counter != null) return counter;

        synchronized (lock) {
            if (counter == null)
                counter = new SingleProcessCounter();
        }
        return counter;
    }

    public void increment(TopicPartition tp, Long delta) {
        long bufferValue = mMessageLocalCounter.merge(tp, delta, (v_old, v_delta) -> v_old + v_delta);

        if (LOG.isDebugEnabled())
            LOG.debug("Topic {} Partition {} local message {}", tp.getTopic(), tp.getPartition(), bufferValue);

    }

    public void decrement(TopicPartition tp, Long delta) {
        long bufferValue = mMessageLocalCounter.merge(tp, delta, (v_old, v_delta) -> v_old - v_delta);

        if (LOG.isDebugEnabled())
            LOG.debug("Topic {} Partition {} local message {}", tp.getTopic(), tp.getPartition(), bufferValue);
    }

    public void topicUploaded(TopicPartition tp) {
        long counter = getLocalCounter(tp);
        mMessageUploadCounter.merge(tp, counter, (v_old, v_delta) -> v_old + v_delta);
        decrement(tp, counter);
    }

    public long getLocalCounter(TopicPartition tp) {
        return mMessageLocalCounter.getOrDefault(tp, 0l);
    }

    public long getTotalCounter(TopicPartition tp) {
        return mMessageLocalCounter.values().stream().reduce((a, b) -> a + b).orElse(0l) + mMessageUploadCounter.values().stream().reduce((a, b) -> a + b).orElse(0l);

    }

    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append("Message completed stats: \n");
        toString(mMessageLocalCounter, sb, "Current local Msg written counter: ");
        toString(mMessageUploadCounter, sb, "Uploaded Msg counter ");

        return sb.toString();
    }

    private void toString(Map<TopicPartition, Long> map, StringBuilder sb, String msg) {
        map.forEach((tp, offset) -> {
            sb
                    .append("[")
                    .append(tp.toString())
                    .append("," + msg + offset)
                    .append("]")
                    .append("\n");
        });
    }

    public void resetLocalCount(TopicPartition topicPartition) {
        mMessageLocalCounter.merge(topicPartition, 0l, (v_old, v_set) -> v_set);
    }
}
