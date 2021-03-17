package org.apache.gobblin.wmf;

import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;

public class TimestampedByteArrayRecord {
    private byte[] record;
    private long timestamp;

    // record = convertRecord(((DecodeableKafkaRecord<?, D>) message).getValue());
    public TimestampedByteArrayRecord(byte[] record, long timestamp) {
        this.record = record;
        this.timestamp = timestamp;
    }
}
