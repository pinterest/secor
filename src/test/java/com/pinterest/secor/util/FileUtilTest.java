/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.util;

import org.mockito.Mockito;
import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;

import com.pinterest.secor.common.SecorConfig;

public class FileUtilTest {

    private SecorConfig mSwiftConfig;
    private SecorConfig mS3Config;

    @Before
    public void setUp() throws Exception {
        mSwiftConfig = Mockito.mock(SecorConfig.class);
        Mockito.when(mSwiftConfig.getCloudService()).thenReturn("Swift");
        Mockito.when(mSwiftConfig.getSeperateContainersForTopics()).thenReturn(false);
        Mockito.when(mSwiftConfig.getSwiftContainer()).thenReturn("some_container");
        Mockito.when(mSwiftConfig.getSwiftPath()).thenReturn("some_swift_parent_dir");

        mS3Config = Mockito.mock(SecorConfig.class);
        Mockito.when(mS3Config.getCloudService()).thenReturn("S3");
        Mockito.when(mS3Config.getS3Bucket()).thenReturn("some_bucket");
        Mockito.when(mS3Config.getS3Path()).thenReturn("some_s3_parent_dir"); 
    }

    @Test
    public void testGetPrefix() throws Exception {
        //FileUtil.configure(mSwiftConfig);
        Assert.assertEquals(FileUtil.getPrefix("some_topic", mSwiftConfig),
                "swift://some_container.GENERICPROJECT/some_swift_parent_dir");

        //FileUtil.configure(mS3Config);
        Assert.assertEquals(FileUtil.getPrefix("some_topic", mS3Config),
                "s3n://some_bucket/some_s3_parent_dir");

        // return to the previous state
        FileUtil.configure(null);
    }
}
