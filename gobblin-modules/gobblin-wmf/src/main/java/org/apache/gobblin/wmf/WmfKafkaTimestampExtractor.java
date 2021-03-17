package org.apache.gobblin.wmf;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import org.apache.gobblin.kafka.client.Kafka1ConsumerClient;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaExtractor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import java.io.IOException;
import java.text.SimpleDateFormat;

@Slf4j
public class WmfKafkaTimestampExtractor extends KafkaExtractor<Object, Object> {
    private static final String POINTER = "wmf.timestamp.json.pointer";
    private static final String FORMAT = "wmf.timestamp.format";
    public static final String DEFAULT_TIMESTAMP_FORMAT = TimestampFormat.ISO_8601.toString();

    private ObjectMapper cachingMapper;
    private String timestampPointer;
    private TimestampFormat timestampFormat;
    private SimpleDateFormat formatter;

    public WmfKafkaTimestampExtractor(WorkUnitState state) {
        super(state);
        this.cachingMapper = new ObjectMapper();
        this.timestampPointer = state.getProp(POINTER, "/meta/dt");
        String formatStr = state.getProp(FORMAT, DEFAULT_TIMESTAMP_FORMAT);

        try {
            this.timestampFormat = TimestampFormat.valueOf(formatStr);
            formatter = null;
        } catch (IllegalArgumentException iae) {
            this.timestampFormat = TimestampFormat.CUSTOM;
            formatter = new SimpleDateFormat(formatStr);
        }
    }

    @Override
    protected TimestampedByteArrayRecord decodeRecord(ByteArrayBasedKafkaRecord kafkaConsumerRecord) {
        return new TimestampedByteArrayRecord(kafkaConsumerRecord.getMessageBytes(), System.currentTimeMillis());
    }

    @Override
    public Object getSchema() {
        return null;
    }

    /**
     * Convert a record to the output format, parsing the timestamp as described
     * @param record the input record
     * @return the converted record
     */
    @Override
    protected TimestampedByteArrayRecord convertRecord(Object record) {
        Kafka1ConsumerClient.Kafka1ConsumerRecord<?, byte[]> typedRecord = (Kafka1ConsumerClient.Kafka1ConsumerRecord)record;
        byte[] jsonPayload = typedRecord.getValue();
        long current = System.currentTimeMillis();
        long timestamp;

        switch (typedRecord.getTimestampType()) {
            // we don't trust log-append times
            case LOG_APPEND_TIME:
                timestamp = WmfKafkaTimestampExtractor.findTimestamp(jsonPayload, current, cachingMapper, timestampPointer, timestampFormat, formatter);
                break;
            // trust create times
            case CREATE_TIME:
                timestamp = typedRecord.getTimestamp();
                break;
            default:
                timestamp = current;
                break;
        }

        return new TimestampedByteArrayRecord(jsonPayload, timestamp);
    }


    /**
     * Uses Jackson databind and JsonPointer syntax to find the timestamp,
     * and uses the configured timestamp format to parse it out
     *   see https://tools.ietf.org/html/draft-ietf-appsawg-json-pointer-03#section-5
     * @param json
     * @param ifNotFound
     * @param cachingMapper
     * @param jsonPointer
     * @param format
     * @param formatter
     * @return
     */
    public static long findTimestamp(byte[] json, long ifNotFound, ObjectMapper cachingMapper, String jsonPointer, TimestampFormat format, SimpleDateFormat formatter) {
        JsonNode root;
        try {
            root = cachingMapper.readTree(json);
        } catch (IOException ie) {
            return ifNotFound;
        }

        // if this throws an exception, it means the jsonPointer is wrong, so no parsing would ever work
        JsonNode match = root.at(jsonPointer);

        try {
            if (match.canConvertToLong()) {
                long found = match.asLong();
                switch (format) {
                    case unix:
                    case unix_seconds:
                        return found * 1000L;
                    case unix_milliseconds:
                        return found;
                    default:
                        return ifNotFound;
                }
            } else if (match.isTextual()) {
                String found = match.asText();
                switch (format) {
                    case ISO_8601:
                        return new DateTime(found).getMillis();
                    case CUSTOM:
                        return formatter.parse(found).getTime();
                    default:
                        return ifNotFound;
                }
            }
        } catch (Exception ex) {
            return ifNotFound;
        }
        return ifNotFound;
    }

    public enum TimestampFormat {
        unix,
        unix_seconds,
        unix_milliseconds,
        ISO_8601,
        CUSTOM
    }
}
