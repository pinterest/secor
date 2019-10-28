package com.pinterest.secor.rebalance;

import com.pinterest.secor.common.FileRegistry;
import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.uploader.Uploader;
import junit.framework.TestCase;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;


public class RebalanceHandlerTest extends TestCase {

    private RebalanceHandler testee;

    private Uploader uploader;

    private FileRegistry fileRegistry;

    private OffsetTracker offsetTracker;

    @Override
    public void setUp() throws Exception {
        fileRegistry = mock(FileRegistry.class);
        offsetTracker = mock(OffsetTracker.class);
        uploader = mock(Uploader.class);
        testee = new RebalanceHandler(uploader, fileRegistry, offsetTracker);

    }

    @Test
    public void testRebalanceHandlerInvoke() throws Exception {

        List<TopicPartition> topicPartitions = new LinkedList<>();
        topicPartitions.add(new TopicPartition("some_topic", 0));
        topicPartitions.add(new TopicPartition("some_topic", 1));
        testee.uploadOnRevoke(topicPartitions);


        Mockito.verify(uploader, times(1)).applyPolicy(true);
        Mockito.verify(fileRegistry, times(2)).deleteTopicPartition(any(TopicPartition.class));
        Mockito.verify(offsetTracker, times(1)).reInitiateOffset();


    }
}
