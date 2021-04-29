package org.apache.gobblin.wmf.kafka.source.extractor.extract.kafka;

import com.google.common.base.Optional;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.Kafka1ConsumerClient;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;
import org.apache.gobblin.source.extractor.extract.kafka.MultiLongWatermark;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUtils {
    public static WorkUnitState getMockWorkUnitState(Optional<String> extractedTimestampTypes,
                                               Optional<String> useCurrentTimestampAsDefault,
                                               String testTopicName) {
        WorkUnit mockWorkUnit = WorkUnit.createEmpty();
        mockWorkUnit.setWatermarkInterval(new WatermarkInterval(new MultiLongWatermark(new ArrayList<Long>(){{add(0L);}}),
                new MultiLongWatermark(new ArrayList<Long>(){{add(10L);}})));

        WorkUnitState mockWorkUnitState = new WorkUnitState(mockWorkUnit, new State());
        mockWorkUnitState.setProp(KafkaSource.TOPIC_NAME, testTopicName);
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

    public static Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> getMockKafkaConsumerRecord(
            TimestampType timestampType, long timestamp, byte[] payload, String testTopicName) {
        ConsumerRecord mockConsumerRecord = mock(ConsumerRecord.class);
        // Values used in base Extractor initialization - using dummies
        when(mockConsumerRecord.offset()).thenReturn(0L);
        when(mockConsumerRecord.serializedValueSize()).thenReturn(0);
        when(mockConsumerRecord.topic()).thenReturn(testTopicName);
        when(mockConsumerRecord.partition()).thenReturn(1);

        // Values used in decodeKafkaMessage - Using parameter values
        when(mockConsumerRecord.timestampType()).thenReturn(timestampType);
        when(mockConsumerRecord.timestamp()).thenReturn(timestamp);
        when(mockConsumerRecord.value()).thenReturn(payload);

        return new Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object>(mockConsumerRecord);
    }
}
