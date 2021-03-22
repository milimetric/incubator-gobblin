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


import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;
import org.apache.gobblin.util.DatasetFilterUtils;

import java.util.List;
import java.util.regex.Pattern;

public abstract class KafkaSource<S, D> extends org.apache.gobblin.source.extractor.extract.kafka.KafkaSource<S, D> {

    @Override
    protected List<KafkaTopic> getFilteredTopics(SourceState state) {
        List<Pattern> blacklist = DatasetFilterUtils.getPatternList(state, TOPIC_BLACKLIST);
        List<Pattern> whitelist = DatasetFilterUtils.getPatternList(state, TOPIC_WHITELIST);
        return this.kafkaConsumerClient.get().getFilteredTopics(blacklist, whitelist);
    }

}