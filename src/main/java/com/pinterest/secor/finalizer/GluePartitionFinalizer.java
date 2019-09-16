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
package com.pinterest.secor.finalizer;

import java.io.IOException;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.finalizer.metastore.GlueClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GluePartitionFinalizer extends AbstractPartitionFinalizer {

  private static final Logger     LOG = LoggerFactory.getLogger(GluePartitionFinalizer.class);
  private              GlueClient glueClient;

  public GluePartitionFinalizer(SecorConfig config) throws Exception {
    super(config);
    glueClient = new GlueClient(config);
  }

  /**
   * Adding Glue Partition
   *
   * @param topic
   * @param partition
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void addPartition(String topic, String partition) throws IOException {
    LOG.info("Glue partition string: " + partition);
    glueClient.addPartition(topic, partition);

  }

}
