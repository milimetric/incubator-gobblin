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

package org.apache.gobblin.wmf.writer.partitioner;

import com.google.common.base.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.wmf.TimestampedRecord;
import org.apache.gobblin.wmf.writer.TimestampedStringRecordDataWriterBuilder;
import org.apache.gobblin.writer.*;
import org.apache.gobblin.writer.partitioner.TimeBasedWriterPartitioner;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;


/**
 * Tests for {@link TimestampedRecordTimeBasedWriterPartitioner}.
 * Copied and updated from gobblin-core:org.apache.gobblin.writer.partitioner.TimeBasedAvroWriterPartitionerTest
 */
@Test
public class TestTimestampedRecordTimeBasedWriterPartitioner {

    private static final String SIMPLE_CLASS_NAME = TestTimestampedRecordTimeBasedWriterPartitioner.class.getSimpleName();

    private static final String TEST_ROOT_DIR = SIMPLE_CLASS_NAME + "-test";
    private static final String STAGING_DIR = TEST_ROOT_DIR + Path.SEPARATOR + "staging";
    private static final String OUTPUT_DIR = TEST_ROOT_DIR + Path.SEPARATOR + "output";
    private static final String BASE_FILE_PATH = "base";
    private static final String FILE_NAME = SIMPLE_CLASS_NAME + "-name.txt";
    private static final String WRITER_ID = "writer-1";

    @BeforeClass
    public void setUp() throws IOException {
        File stagingDir = new File(STAGING_DIR);
        File outputDir = new File(OUTPUT_DIR);

        if (!stagingDir.exists()) {
            stagingDir.mkdirs();
        } else {
            FileUtils.deleteDirectory(stagingDir);
        }

        if (!outputDir.exists()) {
            outputDir.mkdirs();
        } else {
            FileUtils.deleteDirectory(outputDir);
        }
    }

    /**
     * Test
     *  1. Record timestamp of type long
     *  2. Partition path of a given record
     */
    @Test
    public void testWriter() throws IOException {

        State state = getBasicState();
        // Write three records, each should be written to a different file
        DataWriter<Object> millisPartitionWriter = getWriter(state);

        String emptyPayload = "";
        // This timestamp corresponds to 2015/01/01
        millisPartitionWriter.writeEnvelope(new RecordEnvelope<>(new TimestampedRecord<>(emptyPayload, Optional.of(1420099200000L))));

        // This timestamp is empty, leading to current time set
        DateTime currentDateTime = new DateTime(DateTimeZone.UTC);
        millisPartitionWriter.writeEnvelope(new RecordEnvelope<>(new TimestampedRecord<>(emptyPayload, Optional.absent())));

        millisPartitionWriter.close();
        millisPartitionWriter.commit();
        // Check that the writer reports that 2 records have been written
        Assert.assertEquals(millisPartitionWriter.recordsWritten(), 2);

        state.setProp(TimeBasedWriterPartitioner.WRITER_PARTITION_TIMEUNIT, "seconds");
        DataWriter<Object> secsPartitionWriter = getWriter(state);
        // This timestamp corresponds to
        secsPartitionWriter.writeEnvelope(new RecordEnvelope<>(new TimestampedRecord<>(emptyPayload, Optional.of(1420272000L))));
        secsPartitionWriter.close();
        secsPartitionWriter.commit();
        // Check that the writer reports that 1 record has been written
        Assert.assertEquals(secsPartitionWriter.recordsWritten(), 1);

        // Check that 3 files were created
        Assert.assertEquals(FileUtils.listFiles(new File(TEST_ROOT_DIR), new String[] { "txt" }, true).size(), 3);

        // Check if each file exists, and in the correct location
        File baseOutputDir = new File(OUTPUT_DIR, BASE_FILE_PATH);
        Assert.assertTrue(baseOutputDir.exists());

        File outputDir20150101 =
                new File(baseOutputDir, "2015" + Path.SEPARATOR + "01" + Path.SEPARATOR + "01" + Path.SEPARATOR + FILE_NAME);
        Assert.assertTrue(outputDir20150101.exists());

        String currentDatePath = DateTimeFormat.forPattern("yyyy" + Path.SEPARATOR + "MM" + Path.SEPARATOR + "dd").print(currentDateTime);
        File outputDirCurrent =
                new File(baseOutputDir,currentDatePath + Path.SEPARATOR + FILE_NAME);
        Assert.assertTrue(outputDirCurrent.exists());


        File outputDir20150103 =
                new File(baseOutputDir, "2015" + Path.SEPARATOR + "01" + Path.SEPARATOR + "03" + Path.SEPARATOR + FILE_NAME);

        // This test fails because it's multiplying millis by 1000 so it's some date in the far future
        Assert.assertTrue(outputDir20150103.exists());
    }

    @AfterClass
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File(TEST_ROOT_DIR));
    }

    private DataWriter<Object> getWriter(State state)
            throws IOException {
        // Build a writer to write test records
        DataWriterBuilder<String, Object> builder = new TimestampedStringRecordDataWriterBuilder()
                .writeTo(Destination.of(Destination.DestinationType.HDFS, state)).writeInFormat(WriterOutputFormat.TXT)
                .withWriterId(WRITER_ID).withBranches(1).forBranch(0);
        return new PartitionedDataWriter<>(builder, state);
    }

    private State getBasicState() {
        State properties = new State();

        properties.setProp(ConfigurationKeys.SIMPLE_WRITER_DELIMITER, "\n");
        properties.setProp(ConfigurationKeys.WRITER_BUFFER_SIZE, ConfigurationKeys.DEFAULT_BUFFER_SIZE);
        properties.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);
        properties.setProp(ConfigurationKeys.WRITER_STAGING_DIR, STAGING_DIR);
        properties.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, OUTPUT_DIR);
        properties.setProp(ConfigurationKeys.WRITER_FILE_PATH, BASE_FILE_PATH);
        properties.setProp(ConfigurationKeys.WRITER_FILE_NAME, FILE_NAME);
        properties.setProp(TimeBasedWriterPartitioner.WRITER_PARTITION_PATTERN, "yyyy/MM/dd");
        properties.setProp(ConfigurationKeys.WRITER_PARTITIONER_CLASS, TimestampedRecordTimeBasedWriterPartitioner.class.getName());

        return properties;
    }

}
