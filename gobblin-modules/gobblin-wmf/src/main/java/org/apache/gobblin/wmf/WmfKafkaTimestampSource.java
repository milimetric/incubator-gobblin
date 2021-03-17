package org.apache.gobblin.wmf;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;


public class WmfKafkaTimestampSource extends KafkaSource<Object, Object> {
    @Override
    public Extractor<Object, Object> getExtractor(WorkUnitState state) {
        return new WmfKafkaTimestampExtractor(state);
    }
}
