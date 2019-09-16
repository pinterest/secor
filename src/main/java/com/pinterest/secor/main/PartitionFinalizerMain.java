/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pinterest.secor.main;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.finalizer.AbstractPartitionFinalizer;
import com.pinterest.secor.finalizer.GluePartitionFinalizer;
import com.pinterest.secor.finalizer.HivePartitionFinalizer;
import com.pinterest.secor.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partition finalizer main.
 *
 * Run:
 *     $ cd optimus/secor
 *     $ mvn package
 *     $ cd target
 *     $ java -ea -Dlog4j.configuration=log4j.dev.properties -Dconfig=secor.dev.backup.properties \
 *         -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.PartitionFinalizerMain
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class PartitionFinalizerMain {
    private static final Logger LOG = LoggerFactory.getLogger(LogFilePrinterMain.class);

    public static void main(String[] args) {
        try {
            SecorConfig config = SecorConfig.load();
            FileUtil.configure(config);
            String metastore = config.getMetastoreToUpdate();
            AbstractPartitionFinalizer partitionFinalizer;
            if(metastore.equalsIgnoreCase("Glue")){
                partitionFinalizer = new GluePartitionFinalizer(config);
            }else{
                //Keeping the default configuration
                partitionFinalizer = new HivePartitionFinalizer(config);
            }
            partitionFinalizer.finalizePartitions();
        } catch (Throwable t) {
            LOG.error("Partition finalizer failed", t);
            System.exit(1);
        }
    }
}
