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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.marklogic.processor.ExecuteScriptMarkLogicTest.TestExecuteScriptMarkLogic;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.io.Format;

public class PutMarkLogicRecordTest extends AbstractMarkLogicProcessorTest {

    private TestPutMarkLogicRecord processor;
    private MockRecordParser recordReader;
    private MockRecordWriter recordWriter;

    @Before
    public void setup() throws InitializationException {
        processor = new TestPutMarkLogicRecord();
        initialize(processor);
        runner.setProperty(TestExecuteScriptMarkLogic.DATABASE_CLIENT_SERVICE, databaseClientServiceIdentifier);
        recordReader = new MockRecordParser();
        recordReader.addSchemaField("docID", RecordFieldType.STRING);
        recordWriter = new MockRecordWriter("\"docID\"");
    }

    @After
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

        assertTrue("Defaults to true to match what RecordReader does by default", testRecordReaderFactory.testRecordReader.coerceRecordTypes);
        assertTrue("Defaults to true to match what RecordReader does by default", testRecordReaderFactory.testRecordReader.dropUnknownFields);
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

        runner.setProperty(PutMarkLogicRecord.FORMAT, Format.JSON.name());
        runner.setProperty(PutMarkLogicRecord.URI_FIELD_NAME, "docID");
        runner.setProperty(PutMarkLogicRecord.URI_PREFIX, "/prefix/");
        runner.setProperty(PutMarkLogicRecord.URI_SUFFIX, "/suffix.json");
        String[] expectedUris = new String[] {
           "/prefix/123/suffix.json",
           "/prefix/456/suffix.json"
        };

        recordReader.addRecord("123");
        recordReader.addRecord("456");

        runner.enqueue(new byte[0]);

        runner.run();

        assertEquals(processor.writeEvents.size(), 2);
        for (String expectedUri:expectedUris) {
            Stream<WriteEvent> writeEventStream = processor.writeEvents.parallelStream();
            assertTrue(
                writeEventStream.anyMatch((writeEvent) -> {
                    return writeEvent.getTargetUri().equals(expectedUri);
                })
            );
        }
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
/**
 * This subclass allows us to intercept the calls to WriteBatcher so that no calls are made to MarkLogic.
 */
class TestPutMarkLogicRecord extends PutMarkLogicRecord {

    public boolean flushAsyncCalled = false;
    public List<WriteEvent> writeEvents = new ArrayList<WriteEvent>();

    @Override
    protected void flushWriteBatcherAsync(WriteBatcher writeBatcher) {
        flushAsyncCalled = true;
    }

    @Override
    protected void addWriteEvent(WriteBatcher writeBatcher, WriteEvent writeEvent) {
        this.writeEvents.add(writeEvent);
    }
}

class TestRecordReaderFactory extends AbstractControllerService implements RecordReaderFactory {

    public TestRecordReader testRecordReader = new TestRecordReader();

    @Override
    public RecordReader createRecordReader(Map<String, String> map, InputStream inputStream, ComponentLog componentLog) {
        return testRecordReader;
    }

    @Override
    public RecordReader createRecordReader(FlowFile flowFile, InputStream in, ComponentLog logger) {
        return testRecordReader;
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