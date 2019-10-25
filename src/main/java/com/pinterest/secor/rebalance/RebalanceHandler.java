package com.pinterest.secor.rebalance;

import com.pinterest.secor.common.FileRegistry;
import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.uploader.Uploader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class RebalanceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SecorConsumerRebalanceListener.class);

    private Uploader uploader;

    private FileRegistry mfileRegistery;

    private OffsetTracker mOffsetTracker;

    public RebalanceHandler(Uploader uploader, FileRegistry mfileRegistery, OffsetTracker mOffsetTracker) {
        this.uploader = uploader;
        this.mfileRegistery = mfileRegistery;
        this.mOffsetTracker = mOffsetTracker;
    }

    public void uploadOnRevoke(Collection<TopicPartition> assignedPartitions) {
        try {
            uploader.applyPolicy(true);
        } catch (Exception e) {
            LOG.info("re-balance force upload failed, cleaning local files now");
        } finally {
            for (TopicPartition tp : assignedPartitions) {
                forceCleanUp(tp);
            }
            mOffsetTracker.reInitiateOffset();
        }
    }

    private void forceCleanUp(TopicPartition topicPartition) {
        try {
            mfileRegistery.deleteTopicPartition(topicPartition);
        } catch (Exception e) {
            LOG.warn("failure when deleting local files for topic {} and partition {} need to delete it manually", topicPartition.getTopic(), topicPartition.getPartition(), e);
        }
    }

}
