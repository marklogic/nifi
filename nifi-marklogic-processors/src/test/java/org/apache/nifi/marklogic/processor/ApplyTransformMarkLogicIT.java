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
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.marklogic.processor.util.QueryTypes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.management.Query;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ApplyTransformMarkLogicIT extends AbstractMarkLogicIT {

    private String collection;
    private QueryManager queryMgr;

    @BeforeEach
    public void setup() {
        super.setup();
        collection = "ApplyTransformMarkLogicTest";
        queryMgr = getDatabaseClient().newQueryManager();
        loadDocumentsIntoCollection(collection, documents);

        assertTrue(QueryMarkLogic.ORIGINAL.isAutoTerminated(), "For 1.16.3.2, this relationship was marked " +
            "as autoTerminate=true so that ApplyTransformMarkLogic can fix a bug by including the relationship but " +
            "will not break every existing instance of this processor");
    }

    @Test
    public void testApplyTransform() {
        TestRunner runner = getNewTestRunner(ApplyTransformMarkLogic.class);
        runner.setValidateExpressionUsage(false);
        String queryStr = "<cts:element-value-query xmlns:cts=\"http://marklogic.com/cts\">\n" +
            "  <cts:element>sample</cts:element>\n" +
            "  <cts:text xml:lang=\"en\">xmlcontent</cts:text>\n" +
            "</cts:element-value-query>";
        runner.setProperty(QueryMarkLogic.QUERY, queryStr);
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryTypes.COMBINED_XML);
        runner.setProperty(QueryMarkLogic.TRANSFORM, "AddAttribute");
        runner.setProperty("trans:name", "myAttr");
        runner.setProperty("trans:value", "myVal");
        runner.assertValid();

        MockFlowFile mockFlowFile = new MockFlowFile(123);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("hello", "world");
        mockFlowFile.putAttributes(attributes);
        runner.enqueue(mockFlowFile);

        runner.run();

        runner.assertTransferCount(QueryMarkLogic.ORIGINAL, 1);
        assertEquals("world", runner.getFlowFilesForRelationship(QueryMarkLogic.ORIGINAL).get(0).getAttribute("hello"));

        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedXmlCount);
        runner.assertAllFlowFiles(QueryMarkLogic.SUCCESS, flowFile -> {
            assertNotNull(flowFile.getAttribute(CoreAttributes.FILENAME.key()));
            assertEquals("world", flowFile.getAttribute("hello"));
        });

        runner.assertTransferCount(QueryMarkLogic.FAILURE, 0);

        // Verify docs were transformed by having an attribute added
        StringHandle queryHandle = new StringHandle().withFormat(Format.XML).with(queryStr);
        RawCombinedQueryDefinition qDef = queryMgr.newRawCombinedQueryDefinition(queryHandle);
        try (DocumentPage page = getDatabaseClient().newDocumentManager().search(qDef, 1)) {
            page.forEach((docRecord) -> {
                String doc = docRecord.getContentAs(String.class);
                assertTrue(doc.contains("myAttr=\"myVal\""));
            });
        }
    }

    /**
     * Note that when a transform is invalid - either it doesn't exist or cannot be found - the query will still
     * succeed, but every batch will fail. That's due to the transform not being resolved until a batch is processed.
     */
    @Test
    public void invalidTransform() {
        TestRunner runner = getNewTestRunner(ApplyTransformMarkLogic.class);
        runner.setValidateExpressionUsage(false);
        String queryStr = "<cts:element-value-query xmlns:cts=\"http://marklogic.com/cts\">\n" +
            "  <cts:element>sample</cts:element>\n" +
            "  <cts:text xml:lang=\"en\">xmlcontent</cts:text>\n" +
            "</cts:element-value-query>";
        runner.setProperty(QueryMarkLogic.QUERY, queryStr);
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryTypes.COMBINED_XML);
        runner.setProperty(QueryMarkLogic.TRANSFORM, "doesnt-exist");
        runner.assertValid();

        MockFlowFile mockFlowFile = new MockFlowFile(123);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("hello", "world");
        mockFlowFile.putAttributes(attributes);
        runner.enqueue(mockFlowFile);

        runner.run();

        runner.assertTransferCount(QueryMarkLogic.ORIGINAL, 1);
        assertEquals("world", runner.getFlowFilesForRelationship(QueryMarkLogic.ORIGINAL).get(0).getAttribute("hello"));

        runner.assertTransferCount(QueryMarkLogic.SUCCESS, 0);

        runner.assertTransferCount(QueryMarkLogic.FAILURE, expectedXmlCount);
        runner.assertAllFlowFiles(QueryMarkLogic.FAILURE, flowFile -> {
            assertNotNull(flowFile.getAttribute(CoreAttributes.FILENAME.key()));
            assertEquals("world", flowFile.getAttribute("hello"), "Non-core attributes from incoming FlowFile should " +
                "be copied to each failure FlowFile");
        });
    }

    private void loadDocumentsIntoCollection(String collection, List<IngestDoc> documents) {
        WriteBatcher writeBatcher = dataMovementManager.newWriteBatcher()
            .withBatchSize(3)
            .withThreadCount(3);
        dataMovementManager.startJob(writeBatcher);
        for (IngestDoc document : documents) {
            DocumentMetadataHandle handle = new DocumentMetadataHandle();
            handle.withCollections(collection);
            writeBatcher.add(document.getFileName(), handle, new StringHandle(document.getContent()));
        }
        writeBatcher.flushAndWait();
        dataMovementManager.stopJob(writeBatcher);
    }
}
