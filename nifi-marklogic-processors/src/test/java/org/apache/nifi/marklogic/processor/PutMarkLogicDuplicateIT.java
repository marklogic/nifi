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

import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class PutMarkLogicDuplicateIT extends AbstractMarkLogicIT{

	private int modulator = 300;
	private int runSchedule = 1500;

    @BeforeEach
    public void setup() {
    	numDocs = 1200;
        documents = new ArrayList<>(numDocs);
        dataMovementManager = getDatabaseClient().newDataMovementManager();
    	for(int i = 0;i < numDocs;i++) {
    		String content,fileName;
    		fileName = String.format("%03d",(i % modulator)) + ".json";
            content = "{\"id\":"+ i + ", \"dateTime\":\"2000-01-01T00:00:00.000000\"}";
            documents.add(new IngestDoc(fileName, content));
    	}
    }

    public TestRunner getNewTestRunner(Class processor) {
        TestRunner runner = super.getNewTestRunner(processor);
        runner.setThreadCount(4);//Change this to higher value than 1 and likely will fail with XDMP-CONFLICTINGUPDATE
        runner.setProperty(PutMarkLogic.URI_ATTRIBUTE_NAME, "filename");
        runner.setProperty(PutMarkLogic.BATCH_SIZE, "600");
        runner.setProperty(PutMarkLogic.THREAD_COUNT,"8");
        return runner;
    }

    @Test
    public void ingestUsingDefaultIgnoreStrategy() {
    	String collection = "DuplicatePutMarkLogicTest";
        String absolutePath = "/DuplicateUriIgnore/";
        TestRunner runner = getNewTestRunner(PutMarkLogic.class);
        runner.setProperty(PutMarkLogic.COLLECTIONS, collection+",${absolutePath}");
        runner.setProperty(PutMarkLogic.URI_PREFIX, absolutePath);
        //NOT SET to simulate default behavior runner.setProperty(PutMarkLogic.DUPLICATE_URI_HANDLING, PutMarkLogic.FAIL_URI);

        for(IngestDoc document : documents) {
            document.getAttributes().put("absolutePath", absolutePath);
            runner.enqueue(document.getContent(), document.getAttributes());
        }

        runner.run(numDocs);
        runner.setRunSchedule(runSchedule);
        runner.assertQueueEmpty();
        runner.shutdown();

        int dbDocCount = getNumDocumentsInCollection(absolutePath);
        assertEquals("Docs in db should be 0",0,dbDocCount);
        assertEquals("FAILURE should have numDocs",numDocs,runner.getFlowFilesForRelationship(PutMarkLogic.FAILURE).size());
    }

    @Test
    public void ingestUsingFailStrategy() {
        String collection = "DuplicatePutMarkLogicTest";
        String absolutePath = "/DuplicateUriFail/";
        TestRunner runner = getNewTestRunner(PutMarkLogic.class);
        runner.setProperty(PutMarkLogic.COLLECTIONS, collection+",${absolutePath}");
        runner.setProperty(PutMarkLogic.DUPLICATE_URI_HANDLING, PutMarkLogic.FAIL_URI);
        runner.setProperty(PutMarkLogic.URI_PREFIX, absolutePath);

        for(IngestDoc document : documents) {
            document.getAttributes().put("absolutePath", absolutePath);
            runner.enqueue(document.getContent(), document.getAttributes());
        }

        runner.run(numDocs);
        runner.setRunSchedule(runSchedule);
        runner.assertQueueEmpty();
        runner.shutdown();

        int dbDocCount = getNumDocumentsInCollection(absolutePath);
        assertEquals("Docs in db should match modulator",modulator,dbDocCount);
        assertEquals("FAILED_URI should have numDocs - modulator",numDocs - modulator,runner.getFlowFilesForRelationship(PutMarkLogic.DUPLICATE_URI).size());
    }


    @Test
    @Disabled("Disabled in 1.16.3.1; it seems to fail based on changes to NiFi test plumbing which results in the " +
            "processor being shutdown while there are still batches left to process. That causes an NPE when the " +
            "WriteBatcher is accessed. Fixing this should involve simplifying the test - we don't need 1200 docs to be " +
            "created, we just need a small number with a small batch size.")
    public void ingestUsingCloseBatchStrategy() {
        String collection = "DuplicatePutMarkLogicTest";
        String absolutePath = "/DuplicateUriCloseBatch/";
        TestRunner runner = getNewTestRunner(PutMarkLogic.class);
        runner.setProperty(PutMarkLogic.COLLECTIONS, collection+",${absolutePath}");
        runner.setProperty(PutMarkLogic.DUPLICATE_URI_HANDLING, PutMarkLogic.CLOSE_BATCH);
        runner.setProperty(PutMarkLogic.URI_PREFIX, absolutePath);

        for(IngestDoc document : documents) {
            document.getAttributes().put("absolutePath", absolutePath);
            runner.enqueue(document.getContent(), document.getAttributes());
        }

        runner.run(numDocs);
        runner.setRunSchedule(runSchedule);
        runner.assertQueueEmpty();
        runner.shutdown();

        int dbDocCount = getNumDocumentsInCollection(absolutePath);
        assertEquals("Docs in db should match modulator",modulator,dbDocCount);
        assertEquals("Docs in SUCCESS relationship should match numDocs",numDocs,runner.getFlowFilesForRelationship(PutMarkLogic.SUCCESS).size());
        assertEquals("DuplicateUriFlowFileMap should be empty",0,PutMarkLogic.duplicateFlowFileMap.size());
    }
}
