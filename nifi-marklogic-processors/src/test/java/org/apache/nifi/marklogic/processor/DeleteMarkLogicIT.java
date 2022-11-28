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
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.StructuredQueryBuilder;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.marklogic.processor.util.QueryTypes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DeleteMarkLogicIT extends AbstractMarkLogicIT {

    private final static String COLLECTION = "DeleteMarkLogicIT";

    @BeforeEach
    public void setup() {
        super.setup();

        WriteBatcher writeBatcher = dataMovementManager.newWriteBatcher()
            .withBatchSize(3)
            .withThreadCount(3);
        dataMovementManager.startJob(writeBatcher);
        for (IngestDoc document : documents) {
            DocumentMetadataHandle handle = new DocumentMetadataHandle();
            handle.withPermission("rest-reader", DocumentMetadataHandle.Capability.READ);
            handle.withPermission("rest-writer", DocumentMetadataHandle.Capability.UPDATE);
            handle.withCollections(COLLECTION);
            writeBatcher.add(document.getFileName(), handle, new StringHandle(document.getContent()));
        }
        writeBatcher.flushAndWait();
        dataMovementManager.stopJob(writeBatcher);
    }

    @Test
    public void testSimpleCollectionDelete() {
        TestRunner runner = newWriterTestRunner(DeleteMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, COLLECTION);
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryTypes.COLLECTION);
        runner.assertValid();

        MockFlowFile incomingFlowFile = new MockFlowFile(12345);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("hello", "world");
        incomingFlowFile.putAttributes(attributes);

        runner.enqueue(incomingFlowFile);
        runner.run();

        runner.assertTransferCount(QueryMarkLogic.SUCCESS, numDocs);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS, CoreAttributes.FILENAME.key());
        runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS).forEach(flowFile -> {
            assertEquals("world", flowFile.getAttribute("hello"));
        });

        runner.assertTransferCount(QueryMarkLogic.ORIGINAL, 1);
        MockFlowFile originalFlowFile = runner.getFlowFilesForRelationship(QueryMarkLogic.ORIGINAL).get(0);
        assertEquals(12345, originalFlowFile.getId(), "If a FlowFile is passed to DeleteML/QueryML, it is expected to be sent to the " +
            "ORIGINAL relationship before the job completes");

        try (DocumentPage page = getDatabaseClient().newDocumentManager().search(new StructuredQueryBuilder().collection(COLLECTION), 1)) {
            assertEquals(0, page.getTotalSize());
        }
    }

    @Test
    public void deleteWithNoIncomingFlowFile() {
        TestRunner runner = newWriterTestRunner(DeleteMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, COLLECTION);
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryTypes.COLLECTION);
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(QueryMarkLogic.SUCCESS, numDocs);
        runner.assertTransferCount(QueryMarkLogic.ORIGINAL, 1);
        MockFlowFile originalFlowFile = runner.getFlowFilesForRelationship(QueryMarkLogic.ORIGINAL).get(0);
        assertNotNull(originalFlowFile.getAttribute("marklogic-query"), "The marklogic-query attribute should still be " +
            "set on the original FlowFile when no incoming one exists");

        Map<String, String> attributes = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS).get(0).getAttributes();
        // If this count changes in the future, it may simply be due to a new NiFi release that includes an additional
        // key. In the meantime, this is a useful sanity check.
        assertEquals(3, attributes.size(), "Expecting each new FF to have 3 keys - path, filename, and uuid");
        assertTrue(attributes.containsKey("path"));
        assertTrue(attributes.containsKey("filename"));
        assertTrue(attributes.containsKey("uuid"));
    }

    @Test
    public void userCanReadButNotUpdate() {
        TestRunner runner = newReaderTestRunner(DeleteMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, COLLECTION);
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryTypes.COLLECTION);
        runner.assertValid();

        MockFlowFile incomingFlowFile = new MockFlowFile(12345);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("hello", "world");
        incomingFlowFile.putAttributes(attributes);

        runner.enqueue(incomingFlowFile);
        runner.run();

        // The incoming FlowFile should still be sent along, as the processor doesn't know if only some docs fail to be
        // deleted
        runner.assertTransferCount(QueryMarkLogic.ORIGINAL, 1);

        runner.assertTransferCount(QueryMarkLogic.SUCCESS, 0);

        runner.assertTransferCount(QueryMarkLogic.FAILURE, numDocs);
        runner.getFlowFilesForRelationship(QueryMarkLogic.FAILURE).forEach(flowFile -> {
            assertEquals("world", flowFile.getAttribute("hello"));
            String message = flowFile.getAttribute("markLogicErrorMessage");
            assertTrue(message.contains("User is not allowed to delete resource at documents"),
                "Unexpected message: " + message);
        });
    }
}
