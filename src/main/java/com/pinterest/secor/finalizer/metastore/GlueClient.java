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
package com.pinterest.secor.finalizer.metastore;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.*;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.aws.AssumeRoleAndDefaultCredentialsProviderChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Glue client encapsulates communication with AWS GLUE service.
 */
public class GlueClient {

  private static final Logger LOG = LoggerFactory.getLogger(GlueClient.class);

  private final String databaseName;

  private AWSGlue client;

  public GlueClient(SecorConfig config) {
    databaseName = config.getGlueDatabaseName();
    client = AWSGlueClientBuilder
      .standard()
      .withRegion(Regions.fromName(config.getAwsRegion()))
      .withCredentials(new AssumeRoleAndDefaultCredentialsProviderChain())
      .withClientConfiguration(new ClientConfiguration().withRequestTimeout(config.getGlueTimeoutMs()))
      .build();
  }

  public void addPartition(String table, String partition) {

    String[] partitions = partition.split("/");
    String firstPartition = partitions[0].split("=")[1];
    String secondPartition = partitions[1].split("=")[1];

    GetTableRequest getTableRequest = new GetTableRequest();
    getTableRequest.setDatabaseName(databaseName);
    getTableRequest.setName(table);
    GetTableResult glueTable = client.getTable(getTableRequest);
    //FIXME : this fails if the table does not exist
    StorageDescriptor storageDescriptor = glueTable.getTable().getStorageDescriptor();
    storageDescriptor.setLocation(storageDescriptor.getLocation() + "/" + partition);

    CreatePartitionRequest request = new CreatePartitionRequest()
      .withDatabaseName(databaseName)
      .withPartitionInput(new PartitionInput().withValues(firstPartition, secondPartition)
                                              .withStorageDescriptor(storageDescriptor))
      .withTableName(table);
    try {
      client.createPartition(request);
    } catch (AlreadyExistsException e) {
      LOG.warn("AlreadyExistsException: Partition already exists. Authorized. Skipping : " + request.toString());
    }
  }
}
