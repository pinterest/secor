////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
 * Copyright © 2017 Unified Social, Inc.
 * 180 Madison Avenue, 23rd Floor, New York, NY 10016, U.S.A.
 * All rights reserved.
 *
 * This software (the "Software") is provided pursuant to the license agreement you entered into with Unified Social,
 * Inc. (the "License Agreement").  The Software is the confidential and proprietary information of Unified Social,
 * Inc., and you shall use it only in accordance with the terms and conditions of the License Agreement.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND "AS AVAILABLE."  UNIFIED SOCIAL, INC. MAKES NO WARRANTIES OF ANY KIND, WHETHER
 * EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO THE IMPLIED WARRANTIES AND CONDITIONS OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT.
 */

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

package com.unified.secor.io.impl;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.pinterest.secor.common.SecorConfig;
import net.minidev.json.JSONArray;

public class TagToColumns {
    private static final Logger LOG = LoggerFactory.getLogger(TagToColumns.class);
    private static final String SPECIFICATION_OVERRIDE_DIRECTORY = "jolt.spec.override";
    private static final String SPECIFICATIONS_DIRECTORY = "JoltSpecifications";
    private static final Configuration JSON_PATH_CONFIG = Configuration
        .defaultConfiguration()
        .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
        .addOptions(Option.SUPPRESS_EXCEPTIONS);

    public final Map<String, List<String>> tagToColumnMap;

    private TagToColumns () {
        try {
            String specificationDir = SecorConfig.load().getString(
                SPECIFICATION_OVERRIDE_DIRECTORY, SPECIFICATIONS_DIRECTORY);
            this.tagToColumnMap = loadTagToTransformerMap(specificationDir);
        } catch (ConfigurationException e) {
            throw new RuntimeException("Error loading secor configuration");
        }
    }

    private static class TagToColumnsHolder {
        private static final TagToColumns INSTANCE = new TagToColumns();
    }

    public static TagToColumns getInstance () {
        return TagToColumnsHolder.INSTANCE;
    }

    static Map<String, List<String>> loadTagToTransformerMap (String directoryPath) {
        Map<String, List<String>> tagToTransformMap = new HashMap<String, List<String>>();
        File                      directory         = new File(directoryPath);
        FilenameFilter            jsonFileFilter    = new SuffixFileFilter(".json");
        File[]                    files             = directory.listFiles(jsonFileFilter);

        if ( files == null ) {
            String errorMessage = "Invalid specification directory supplied.";
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        for (File file : files) {
            DocumentContext parsedPayload;
            try {
                parsedPayload = JsonPath
                    .using(JSON_PATH_CONFIG)
                    .parse(file);
            }
            catch (IOException e) {
                throw new RuntimeException("Unable to load specification file", e);
            }

            JSONArray columnJsonArray = parsedPayload.read("$..metadata.columns");

            ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
            for (Object o :((Map) columnJsonArray.get(0)).keySet()) {
                listBuilder.add(o.toString());
            }

            List<String> columns = listBuilder.build();
            JSONArray tagArray = parsedPayload.read("$..metadata.tags");

            for (Object tag : (JSONArray) tagArray.get(0)) {
                tagToTransformMap.put(tag.toString(), columns);
            }
        }

        return tagToTransformMap;
    }
}
