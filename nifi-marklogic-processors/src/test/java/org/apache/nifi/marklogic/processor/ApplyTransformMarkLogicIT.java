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
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class ApplyTransformMarkLogicIT extends AbstractMarkLogicIT {

    private String collection;
    private QueryManager queryMgr;

    @BeforeEach
    public void setup() {
        super.setup();
        collection = "ApplyTransformMarkLogicTest";
        queryMgr = getDatabaseClient().newQueryManager();
        loadDocumentsIntoCollection(collection, documents);
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
        runner.run();
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedXmlCount);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS, CoreAttributes.FILENAME.key());
        StringHandle queryHandle = new StringHandle().withFormat(Format.XML).with(queryStr);
        RawCombinedQueryDefinition qDef = queryMgr.newRawCombinedQueryDefinition(queryHandle);
        DocumentPage page = getDatabaseClient().newDocumentManager().search(qDef, 1);
        page.forEach((docRecord) -> {
            String doc = docRecord.getContentAs(String.class);
            assertTrue(doc.contains("myAttr=\"myVal\""));
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
