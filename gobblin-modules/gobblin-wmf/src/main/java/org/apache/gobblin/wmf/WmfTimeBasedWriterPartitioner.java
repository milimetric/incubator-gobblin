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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;


/**
 * A {@link TimeBasedWriterPartitioner} for byte[] containing json.
 *
 * The json field that contains the timestamp can be specified using TIMESTAMP_COLUMN_KEY.
 * The format of the json field that contains the timestamp can be specified using TIMESTAMP_FORMAT_KEY.
 *
 * If a record doesn't contain the specified field, or if no field is specified, the current timestamp will be used.
 */
@Slf4j
public class WmfTimeBasedWriterPartitioner extends TimeBasedWriterPartitioner<byte[]> {

    public static final String TIMESTAMP_COLUMN_KEY = ConfigurationKeys.WRITER_PREFIX + ".partition.timestamp.column";
    public static final String DEFAULT_TIMESTAMP_COLUMN = "timestamp";

    public static final String TIMESTAMP_FORMAT_KEY = ConfigurationKeys.WRITER_PREFIX + ".partition.timestamp.format";
    public static final String DEFAULT_TIMESTAMP_FORMAT = "ISO-8601";

    private final String timestampColumn;
    private final String timestampFormat;

    private final JsonParser jsonParser;

    public WmfTimeBasedWriterPartitioner(State state) {
        this(state, 1, 0);
    }

    public WmfTimeBasedWriterPartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);
        this.timestampColumn = getTimestampColumn(state, numBranches, branchId);
        this.timestampFormat = getTimestampFormat(state, numBranches, branchId);

        this.jsonParser = new JsonParser();
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
        return getRecordTimestamp(getWriterPartitionColumnValue(record));
    }

    /**
     *  Check if the partition column value is present and is a Long object. Otherwise, use current system time.
     */
    private long getRecordTimestamp(Optional<JsonPrimitive> timestampPrimitive) {

        if (timestampPrimitive.isPresent() ) {
            // If timestampFormat is 'unix_seconds',
            // then the timestamp only needs converted to milliseconds.
            // Also support 'unix' for backwards compatibility.
            if (timestampFormat.equals("unix_seconds") || timestampFormat.equals("unix")) {
                // This timestamp is in seconds, convert it to milliseconds.
                return timestampPrimitive.get().getAsLong() * 1000L;
            }
            // Else if this timestamp is already in milliseconds,
            // just save it as is.
            else if (timestampFormat.equals("unix_milliseconds")) {
                return timestampPrimitive.get().getAsLong();
            }
            // Else if timestampFormat is 'ISO-8601', parse that
            else {
                String timestampString = timestampPrimitive.get().getAsString();
                if (timestampFormat.equals("ISO-8601")) {
                    try {
                        return new DateTime(timestampString).getMillis();
                    } catch (IllegalArgumentException e) {
                        log.error("Could not parse timestamp '" + timestampString + "' as ISO-8601 while decoding JSON message.");
                    }
                } else {
                    // Otherwise parse the timestamp as a string in timestampFormat
                    try {
                        return new SimpleDateFormat(timestampFormat).parse(timestampString).getTime();
                    } catch (ParseException pe) {
                        log.error("Could not parse timestamp '" + timestampString + "' while decoding JSON message.");
                    }
                }
            }
        }

        // If we reach this point, return current time.
        log.warn("Couldn't find or parse timestamp field '" + timestampColumn
                + "' in JSON message, defaulting to current time.");
        return timeUnit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

    }

    /**
     * Retrieve the value of the partition column field specified by this.partitionColumns
     */
    private Optional<JsonPrimitive> getWriterPartitionColumnValue(byte[] payload) {
        if (this.timestampColumn == null) {
            return Optional.absent();
        }

        String payloadString;
        JsonObject jsonObject;

        try {
            payloadString = new String(payload, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("Unable to load UTF-8 encoding, falling back to system default", e);
            payloadString = new String(payload);
        }

        // Parse the payload into a JsonObject.
        try {
            jsonObject = jsonParser.parse(payloadString.trim()).getAsJsonObject();
        } catch (RuntimeException e) {
            log.error("Caught exception while parsing JSON string '" + payloadString + "'.");
            throw new RuntimeException(e);
        }

        // Find the timestampField value in the jsonObject
        return extractJsonPrimitive(jsonObject, timestampColumn);
    }

    private Optional<JsonPrimitive> extractJsonPrimitive(JsonObject jsonObject, String jsonPath) {
        // if the path has dots, assume the first element in the path is
        // in jsonObject, and the following elements are in subobjects.
        if (jsonPath.contains(".")) {
            String[] paths = jsonPath.split("\\.", 2);
            if (jsonObject.has(paths[0])) {
                return extractJsonPrimitive(jsonObject.getAsJsonObject(paths[0]), paths[1]);
            } else {
                return Optional.absent();
            }
        } else {
            if (jsonObject.has(jsonPath)) {
                return Optional.of(jsonObject.getAsJsonPrimitive(jsonPath));
            } else {
                return Optional.absent();
            }
        }
    }
}
