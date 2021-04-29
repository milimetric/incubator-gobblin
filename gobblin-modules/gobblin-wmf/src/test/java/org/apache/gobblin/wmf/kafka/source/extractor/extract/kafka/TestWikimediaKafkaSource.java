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

import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Tests for {@link WikimediaKafkaSource}
 */

@Slf4j
public class TestWikimediaKafkaSource {

    @Test
    public void testSettingsListToMap() {
        WikimediaKafkaSource source = new WikimediaKafkaSource() {
            @Override
            public Extractor getExtractor(WorkUnitState state) throws IOException {
                return null;
            }
        };

        Map<String, String> settingsMap = source.settingsListToMap(Arrays.asList("set1:val1", "set2:val2", "set3:val3"));
        Assert.assertEquals(settingsMap.get("set2"), "val2");
    }
}