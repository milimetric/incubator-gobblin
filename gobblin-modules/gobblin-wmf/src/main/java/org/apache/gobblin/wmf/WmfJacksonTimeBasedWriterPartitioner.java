/*
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


package org.apache.gobblin.wmf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.gobblin.writer.partitioner.TimeBasedWriterPartitioner;
import org.joda.time.DateTime;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

import static org.apache.parquet.VersionParser.FORMAT;


/**
 * A {@link TimeBasedWriterPartitioner} for byte[] containing json.
 *
 * The json field that contains the timestamp can be specified using TIMESTAMP_COLUMN_KEY.
 * The format of the json field that contains the timestamp can be specified using TIMESTAMP_FORMAT_KEY.
 *
 * If a record doesn't contain the specified field, or if no field is specified, the current timestamp will be used.
 */
@Slf4j
public class WmfJacksonTimeBasedWriterPartitioner extends TimeBasedWriterPartitioner<byte[]> {

    public static final String TIMESTAMP_COLUMN_KEY = ConfigurationKeys.WRITER_PREFIX + ".partition.timestamp.column";
    public static final String DEFAULT_TIMESTAMP_COLUMN = "/timestamp";

    public static final String TIMESTAMP_FORMAT_KEY = ConfigurationKeys.WRITER_PREFIX + ".partition.timestamp.format";
    public static final String DEFAULT_TIMESTAMP_FORMAT = "ISO_8601";

    private final ObjectMapper cachingMapper;
    private final String timestampPointer;
    private SimpleDateFormat formatter;
    private WmfKafkaTimestampExtractor.TimestampFormat timestampFormat;

    public WmfJacksonTimeBasedWriterPartitioner(State state) {
        this(state, 1, 0);
    }

    public WmfJacksonTimeBasedWriterPartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);

        this.timestampPointer = getTimestampColumn(state, numBranches, branchId);
        String formatStr = getTimestampFormat(state, numBranches, branchId);

        this.cachingMapper = new ObjectMapper();

        try {
            this.timestampFormat = WmfKafkaTimestampExtractor.TimestampFormat.valueOf(formatStr);
            formatter = null;
        } catch (IllegalArgumentException iae) {
            this.timestampFormat = WmfKafkaTimestampExtractor.TimestampFormat.CUSTOM;
            formatter = new SimpleDateFormat(formatStr);
        }
    }

    private static String getTimestampColumn(State state, int numBranches, int branchId) {
        String propName = ForkOperatorUtils.getPropertyNameForBranch(TIMESTAMP_COLUMN_KEY, numBranches, branchId);
        String prop = state.getProp(propName, DEFAULT_TIMESTAMP_COLUMN);

        log.info("timestamp column for dataset {} is: {}", state.getProp(ConfigurationKeys.DATASET_URN_KEY), prop);
        return prop;
    }

    private static String getTimestampFormat(State state, int numBranches, int branchId) {
        String propName = ForkOperatorUtils.getPropertyNameForBranch(TIMESTAMP_FORMAT_KEY, numBranches, branchId);
        String prop = state.getProp(propName, DEFAULT_TIMESTAMP_FORMAT);

        log.info("timestamp format for dataset {} is: {}", state.getProp(ConfigurationKeys.DATASET_URN_KEY), prop);
        return prop;
    }


    @Override
    public long getRecordTimestamp(byte[] record) {
        return WmfKafkaTimestampExtractor.findTimestamp(record, System.currentTimeMillis(), cachingMapper, timestampPointer, timestampFormat, formatter);
    }
}
