/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.marklogic.processor;

import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.io.Format;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.marklogic.processor.ExecuteScriptMarkLogicTest.TestExecuteScriptMarkLogic;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class PutMarkLogicRecordTest extends AbstractMarkLogicProcessorTest {

    private TestPutMarkLogicRecord processor;
    private MockRecordParser recordReader;
    private MockRecordWriter recordWriter;

    @BeforeEach
    public void setup() throws InitializationException {
        processor = new TestPutMarkLogicRecord();
        initialize(processor);
        runner.setProperty(TestExecuteScriptMarkLogic.DATABASE_CLIENT_SERVICE, databaseClientServiceIdentifier);
        recordReader = new MockRecordParser();
        recordReader.addSchemaField("docID", RecordFieldType.STRING);
        recordWriter = new MockRecordWriter("\"docID\"");
    }

    @AfterEach
    public void reset() {
        processor.writeEvents.clear();
    }

    /**
     * This test and customRecordReaderInputs rely on a test implementation of RecordReaderFactory so that the
     * inputs passed to the "nextRecord" method can be captured and verified.
     */
    @Test
    public void defaultRecordReaderInputs() {
        TestRecordReaderFactory testRecordReaderFactory = new TestRecordReaderFactory();
        configureRecordReaderFactory(testRecordReaderFactory);
        configureRecordSetWriterFactory(recordWriter);
        configureDatabaseClientService();

        processor.initialize(initializationContext);
        runner.enqueue(new byte[0]);
        runner.run();

        assertTrue(testRecordReaderFactory.testRecordReader.coerceRecordTypes,
            "Defaults to true to match what RecordReader does by default");
        assertTrue(testRecordReaderFactory.testRecordReader.dropUnknownFields,
            "Defaults to true to match what RecordReader does by default");
    }

    @Test
    public void customRecordReaderInputs() {
        TestRecordReaderFactory testRecordReaderFactory = new TestRecordReaderFactory();
        configureRecordReaderFactory(testRecordReaderFactory);
        configureRecordSetWriterFactory(recordWriter);
        configureDatabaseClientService();

        runner.setProperty(PutMarkLogicRecord.RECORD_COERCE_TYPES, "false");
        runner.setProperty(PutMarkLogicRecord.RECORD_DROP_UNKNOWN_FIELDS, "false");

        processor.initialize(initializationContext);
        runner.enqueue(new byte[0]);
        runner.run();

        assertFalse(testRecordReaderFactory.testRecordReader.coerceRecordTypes);
        assertFalse(testRecordReaderFactory.testRecordReader.dropUnknownFields);
    }

    @Test
    public void putRecords() {
        configureRecordReaderFactory(recordReader);
        configureRecordSetWriterFactory(recordWriter);
        configureDatabaseClientService();

        runner.setProperty(PutMarkLogicRecord.FORMAT, Format.TEXT.name());
        runner.setProperty(PutMarkLogicRecord.URI_FIELD_NAME, "docID");
        runner.setProperty(PutMarkLogicRecord.URI_PREFIX, "/prefix/");
        runner.setProperty(PutMarkLogicRecord.URI_SUFFIX, "/suffix.txt");
        String[] expectedUris = new String[]{
            "/prefix/123/suffix.txt",
            "/prefix/456/suffix.txt"
        };

        recordReader.addRecord("123");
        recordReader.addRecord("456");

        runner.enqueue(new byte[0]);

        runner.run();

        assertEquals(processor.writeEvents.size(), 2);
        for (String expectedUri : expectedUris) {
            Stream<WriteEvent> writeEventStream = processor.writeEvents.parallelStream();
            assertTrue(writeEventStream.anyMatch(writeEvent -> writeEvent.getTargetUri().equals(expectedUri)));
        }

        // Verify that a FlowFile was sent to batch_success containing all the URIs in the batch; that the original
        // FlowFile was passed along; and that 2 FlowFiles were sent to "success", one for each URI
        runner.assertTransferCount("batch_success", 1);
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("failure", 0);
        runner.assertTransferCount("success", 2);

        MockFlowFile mockFile = runner.getFlowFilesForRelationship("batch_success").get(0);
        assertEquals(
            "/prefix/123/suffix.txt,/prefix/456/suffix.txt", mockFile.getAttribute("URIs"),
            "The FF sent to batch_success is expected to contain each of the URIs in that batch");
    }

    private void configureRecordReaderFactory(ControllerService recordReaderFactory) {
        try {
            runner.addControllerService("reader", recordReaderFactory);
        } catch (InitializationException e) {
            throw new RuntimeException(e);
        }
        runner.enableControllerService(recordReaderFactory);
        runner.setProperty(PutMarkLogicRecord.RECORD_READER, "reader");
        runner.setProperty(PutMarkLogicRecord.RECORD_READER, "reader");
    }

    private void configureRecordSetWriterFactory(ControllerService recordSetWriterFactory) {
        try {
            runner.addControllerService("writer", recordSetWriterFactory);
        } catch (InitializationException e) {
            throw new RuntimeException(e);
        }
        runner.enableControllerService(recordSetWriterFactory);
        runner.setProperty(PutMarkLogicRecord.RECORD_WRITER, "writer");
        runner.setProperty(PutMarkLogicRecord.RECORD_WRITER, "writer");
    }
}

class TestPutMarkLogicRecord extends PutMarkLogicRecord {

    public List<WriteEvent> writeEvents = new ArrayList<>();

    @Override
    protected void flushWriteBatcherAsync(WriteBatcher writeBatcher) {
        writeBatcher.flushAndWait();
    }

    @Override
    protected void addWriteEvent(WriteBatcher writeBatcher, WriteEvent writeEvent) {
        super.addWriteEvent(writeBatcher, writeEvent);
        this.writeEvents.add(writeEvent);
    }
}

class TestRecordReaderFactory extends AbstractControllerService implements RecordReaderFactory {

    public TestRecordReader testRecordReader = new TestRecordReader();

//    @Override
//    public RecordReader createRecordReader(Map<String, String> map, InputStream inputStream, ComponentLog componentLog) {
//        return testRecordReader;
//    }

    @Override
    public RecordReader createRecordReader(FlowFile flowFile, InputStream in, ComponentLog logger) {
        return testRecordReader;
    }

    @Override
    public RecordReader createRecordReader(Map<String, String> map, InputStream inputStream, long l, ComponentLog componentLog) {
        return null;
    }
}

class TestRecordReader implements RecordReader {

    public boolean coerceRecordTypes;
    public boolean dropUnknownFields;

    @Override
    public Record nextRecord(boolean coerceRecordTypes, boolean dropUnknownFields) {
        this.coerceRecordTypes = coerceRecordTypes;
        this.dropUnknownFields = dropUnknownFields;
        return null;
    }

    @Override
    public RecordSchema getSchema() {
        return new SimpleRecordSchema(new ArrayList<>());
    }

    @Override
    public void close() {
    }
}
