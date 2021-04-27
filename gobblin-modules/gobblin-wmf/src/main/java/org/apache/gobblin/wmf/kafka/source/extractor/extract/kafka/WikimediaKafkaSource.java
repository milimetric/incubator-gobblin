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
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;
import org.apache.gobblin.util.DatasetFilterUtils;
import org.wikimedia.eventutilities.core.event.EventStreamConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class WikimediaKafkaSource<S, D> extends KafkaSource<S, D> {

    private static final String TOPIC_INCLUDE_LIST = "topic.include";
    private static final String EVENT_STREAM_CONFIG_URI = "event_stream_config.uri";
    private static final String EVENT_STREAM_CONFIG_STREAM_NAMES = "event_stream_config.stream_names";
    private static final String EVENT_STREAM_CONFIG_SETTINGS_FILTERS = "event_stream_config.settings_filters";

    /**
     * Finds Kafka topics to ingest.  Defaults to a literal include list.  In absence of that,
     * uses Wikimedia's EventStreamConfig service to get a list of topics based on:
     *  - a configured service URI
     *  - a list of stream names to get topics from
     *  - a list of settings filters in the form key1:value1,key2:value2
     *
     * @param state Work unit state
     * @return List of configured topics
     */
    @Override
    protected List<KafkaTopic> getFilteredTopics(SourceState state) {
        // default to a literal include list
        List<String> includeTopics = state.getPropAsList(TOPIC_INCLUDE_LIST);

        // fetch topics from Wikimedia's EventStreamConfig service
        if (includeTopics == null) {
            final String configUri = state.getProp(EVENT_STREAM_CONFIG_URI);
            final List<String> streamNames = state.getPropAsList(EVENT_STREAM_CONFIG_STREAM_NAMES, "");
            final List<String> settingsFilters = state.getPropAsList(EVENT_STREAM_CONFIG_SETTINGS_FILTERS, "");

            // Get an EventStreamConfig instance using eventStreamConfigUri.
            EventStreamConfig eventStreamConfig = EventStreamConfig.builder()
                    .setEventStreamConfigLoader(configUri)
                    .build();

            // Get the list of topics matching the target streamNames and settingsFilters.
            includeTopics = eventStreamConfig.collectTopicsMatchingSettings(streamNames, settingsListToMap(settingsFilters));
            if (includeTopics.isEmpty()) {
                throw new RuntimeException("Failed to obtain topics to consume from EventStreamConfig at " + configUri);
            }
        }
        List<Pattern> exclude = new ArrayList<>();
        List<Pattern> include = DatasetFilterUtils.getPatternsFromStrings(includeTopics);

        return kafkaConsumerClient.get().getFilteredTopics(exclude, include);
    }

    protected Map<String, String> settingsListToMap(List<String> settingsFilters) {
        return settingsFilters.stream()
            .map(s -> s.split(":"))
            .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }
}