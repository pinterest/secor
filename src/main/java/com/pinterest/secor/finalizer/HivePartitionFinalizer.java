package com.pinterest.secor.finalizer;

import java.io.IOException;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.finalizer.metastore.QuboleClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HivePartitionFinalizer extends AbstractPartitionFinalizer {

    private static final Logger LOG = LoggerFactory.getLogger(HivePartitionFinalizer.class);

    private final QuboleClient mQuboleClient;

    public HivePartitionFinalizer(SecorConfig config) throws Exception {
        super(config);
        mQuboleClient = new QuboleClient(mConfig);
    }


    /**
     * Adding Hive Partition via QuboleClient
     * @param topic
     * @param partition
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void addPartition(String topic, String partition) throws IOException, InterruptedException {
        LOG.info("Hive partition string: " + partition);

        String hiveTableName = mConfig.getHiveTableName(topic);
        LOG.info("Hive table name from config: {}", hiveTableName);
        if (hiveTableName == null) {
            String hivePrefix = null;
            try {
                hivePrefix = mConfig.getHivePrefix();
                hiveTableName = hivePrefix + topic;
                LOG.info("Hive table name from prefix: {}", hiveTableName);
            } catch (RuntimeException ex) {
                LOG.warn("HivePrefix is not defined.  Skip hive registration");
            }
        }
        if (hiveTableName != null && mConfig.getMetastoreEnabled()) {
            mQuboleClient.addPartition(hiveTableName, partition);
        }
    }
}
