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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
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
        recordReader = new MockRecordParser();
        recordReader.addSchemaField("docID", RecordFieldType.STRING);
        recordWriter = new MockRecordWriter("\"docID\"");
    }

    @Test
    public void putRecords() throws InitializationException {
        runner.enableControllerService(service);
        runner.assertValid(service);

        runner.setProperty(PutMarkLogicRecord.DATABASE_CLIENT_SERVICE, databaseClientServiceIdentifier);
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", recordWriter);
        runner.enableControllerService(recordWriter);
        runner.setProperty(PutMarkLogicRecord.RECORD_READER, "reader");
        runner.setProperty(PutMarkLogicRecord.RECORD_WRITER, "writer");
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

    @After
    public void reset() {
        processor.writeEvents.clear();
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