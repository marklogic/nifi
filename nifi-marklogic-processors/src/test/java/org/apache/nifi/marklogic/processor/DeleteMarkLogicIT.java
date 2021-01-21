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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DeleteMarkLogicIT extends AbstractMarkLogicIT {
    private String collection;

    @BeforeEach
    public void setup() {
        super.setup();
        collection = "QueryMarkLogicTest";
        // Load documents to Query
        loadDocumentsIntoCollection(collection, documents);
    }

    private void loadDocumentsIntoCollection(String collection, List<IngestDoc> documents) {
        WriteBatcher writeBatcher = dataMovementManager.newWriteBatcher()
            .withBatchSize(3)
            .withThreadCount(3);
        dataMovementManager.startJob(writeBatcher);
        for(IngestDoc document : documents) {
            DocumentMetadataHandle handle = new DocumentMetadataHandle();
            handle.withCollections(collection);
            writeBatcher.add(document.getFileName(), handle, new StringHandle(document.getContent()));
        }
        writeBatcher.flushAndWait();
        dataMovementManager.stopJob(writeBatcher);
    }

    @Test
    public void testSimpleCollectionDelete() {
        TestRunner runner = getNewTestRunner(DeleteMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, collection);
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COLLECTION);
        runner.assertValid();
        runner.enqueue(new MockFlowFile(12345));
        runner.run();

        runner.assertTransferCount(QueryMarkLogic.SUCCESS, numDocs);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS, CoreAttributes.FILENAME.key());

        runner.assertTransferCount(QueryMarkLogic.ORIGINAL, 1);
        MockFlowFile originalFlowFile = runner.getFlowFilesForRelationship(QueryMarkLogic.ORIGINAL).get(0);
        assertEquals("If a FlowFile is passed to DeleteML/QueryML, it is expected to be sent to the " +
                "ORIGINAL relationship before the job completes", 12345, originalFlowFile.getId());

        DocumentPage page = getDatabaseClient().newDocumentManager().search(new StructuredQueryBuilder().collection(collection), 1);
        assertEquals(0, page.getTotalSize());
    }

    @AfterEach
    public void teardown() {
        deleteDocumentsInCollection(collection);
    }
}
