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


package org.apache.gobblin.wmf.kafka.source.extractor.extract.kafka;

import com.google.common.base.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.Kafka1ConsumerClient;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;
import org.apache.gobblin.source.extractor.extract.kafka.MultiLongWatermark;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.wmf.TimestampedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NoSuchElementException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Kafka1TimestampedRecordExtractor}
 */

@Slf4j
public class TestKafka1TimestampedRecordExtractor {

    private static final String TEST_TOPIC_NAME = "testTopic";
    private static final byte[] testPayload = "Hello World".getBytes();
    private static final long testTimestamp = 1420099200000L;
    private static final Optional<String> noString = Optional.absent();

    @Test
    public void testDecodeKafkaMessageWithoutTimestamp() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = getMockWorkUnitState(noString, noString);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                getMockKafkaConsumerRecord(TimestampType.NO_TIMESTAMP_TYPE, 0L, testPayload);

        TimestampedRecord<byte[]> expected = new TimestampedRecord<>(testPayload, Optional.absent());
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDecodeKafkaMessageWithoutTimestampButCurrentDefault() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = getMockWorkUnitState(noString, Optional.of("true"));
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                getMockKafkaConsumerRecord(TimestampType.NO_TIMESTAMP_TYPE, 0L, testPayload);

        long before = System.currentTimeMillis();
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertTrue(actual.getTimestamp().isPresent());
        Assert.assertTrue(actual.getTimestamp().get() >= before);
    }

    @Test
    public void testDecodeKafkaMessageWitTimestampButNoAcceptedType() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = getMockWorkUnitState(noString, noString);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                getMockKafkaConsumerRecord(TimestampType.CREATE_TIME, testTimestamp, testPayload);

        TimestampedRecord<byte[]> expected = new TimestampedRecord<>(testPayload, Optional.absent());
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDecodeKafkaMessageWitTimestampAndAcceptedType() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = getMockWorkUnitState(Optional.of("CreateTime"), noString);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                getMockKafkaConsumerRecord(TimestampType.CREATE_TIME, testTimestamp, testPayload);

        TimestampedRecord<byte[]> expected = new TimestampedRecord<>(testPayload, Optional.of(testTimestamp));
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDecodeKafkaMessageWitTimestampAndAcceptedTypeList() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = getMockWorkUnitState(Optional.of("CreateTime,LogAppendTime"), noString);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                getMockKafkaConsumerRecord(TimestampType.CREATE_TIME, testTimestamp, testPayload);

        TimestampedRecord<byte[]> expected = new TimestampedRecord<>(testPayload, Optional.of(testTimestamp));
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDecodeKafkaMessageWitTimestampAndAcceptedTypeList2() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = getMockWorkUnitState(Optional.of("CreateTime,LogAppendTime"), noString);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                getMockKafkaConsumerRecord(TimestampType.LOG_APPEND_TIME, testTimestamp, testPayload);

        TimestampedRecord<byte[]> expected = new TimestampedRecord<>(testPayload, Optional.of(testTimestamp));
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDecodeKafkaMessageWithoutTimestampAndAcceptedType() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = getMockWorkUnitState(Optional.of("CreateTime"), noString);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                getMockKafkaConsumerRecord(TimestampType.NO_TIMESTAMP_TYPE, 0l, testPayload);

        TimestampedRecord<byte[]> expected = new TimestampedRecord<>(testPayload, Optional.absent());
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDecodeKafkaMessageWithoutTimestampAndAcceptedTypeAndCurrentTimestampDefault()
            throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = getMockWorkUnitState(Optional.of("CreateTime"), Optional.of("true"));
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                getMockKafkaConsumerRecord(TimestampType.NO_TIMESTAMP_TYPE, 0L, testPayload);

        long before = System.currentTimeMillis();
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertTrue(actual.getTimestamp().isPresent());
        Assert.assertTrue(actual.getTimestamp().get() >= before);
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testWrongExtractedTimestampType() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = getMockWorkUnitState(Optional.of("Wrong"), noString);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidExtractedTimestampType() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = getMockWorkUnitState(Optional.of("NoTimestampType"), noString);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFailDecodeKafkaRecord() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = getMockWorkUnitState(Optional.of("CreateTime,LogAppendTime"), noString);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        kTRE.decodeRecord(null);
    }

    private WorkUnitState getMockWorkUnitState(Optional<String> extractedTimestampTypes,
                                               Optional<String> useCurrentTimestampAsDefault) {
        WorkUnit mockWorkUnit = WorkUnit.createEmpty();
        mockWorkUnit.setWatermarkInterval(new WatermarkInterval(new MultiLongWatermark(new ArrayList<Long>(){{add(0L);}}),
                new MultiLongWatermark(new ArrayList<Long>(){{add(10L);}})));

        WorkUnitState mockWorkUnitState = new WorkUnitState(mockWorkUnit, new State());
        mockWorkUnitState.setProp(KafkaSource.TOPIC_NAME, TEST_TOPIC_NAME);
        mockWorkUnitState.setProp(KafkaSource.PARTITION_ID, "1");
        mockWorkUnitState.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:8080");
        mockWorkUnitState.setProp(KafkaSource.GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS,
                "org.apache.gobblin.kafka.client.Kafka1ConsumerClient$Factory");
        mockWorkUnitState.setProp(Kafka1ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        if (extractedTimestampTypes.isPresent()) {
            mockWorkUnitState.setProp(Kafka1TimestampedRecordExtractor.EXTRACTED_TIMESTAMP_TYPES_KEY,
                    extractedTimestampTypes.get());
        }
        if (useCurrentTimestampAsDefault.isPresent()) {
            mockWorkUnitState.setProp(Kafka1TimestampedRecordExtractor.CURRENT_TIMESTAMP_AS_DEFAULT_KEY,
                    useCurrentTimestampAsDefault.get());
        }

        return mockWorkUnitState;
    }

    private Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> getMockKafkaConsumerRecord(
            TimestampType timestampType, long timestamp, byte[] payload) {
        ConsumerRecord mockConsumerRecord = mock(ConsumerRecord.class);
        // Values used in base Extractor initialization - using dummies
        when(mockConsumerRecord.offset()).thenReturn(0L);
        when(mockConsumerRecord.serializedValueSize()).thenReturn(0);
        when(mockConsumerRecord.topic()).thenReturn(TEST_TOPIC_NAME);
        when(mockConsumerRecord.partition()).thenReturn(1);

        // Values used in decodeKafkaMessage - Using parameter values
        when(mockConsumerRecord.timestampType()).thenReturn(timestampType);
        when(mockConsumerRecord.timestamp()).thenReturn(timestamp);
        when(mockConsumerRecord.value()).thenReturn(payload);

        return new Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object>(mockConsumerRecord);
    }

}