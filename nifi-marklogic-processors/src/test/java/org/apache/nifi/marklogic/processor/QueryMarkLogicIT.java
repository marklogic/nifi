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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.StructuredQueryDefinition;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class QueryMarkLogicIT extends AbstractMarkLogicIT {

    private final static String TEST_COLLECTION = "QueryMarkLogicTest";

    @BeforeEach
    public void insertTestDocumentsForQuerying() {
        super.setup();

        GenericDocumentManager mgr = getDatabaseClient().newDocumentManager();
        DocumentWriteSet writeSet = mgr.newWriteSet();
        for (IngestDoc document : documents) {
            DocumentMetadataHandle metadata = new DocumentMetadataHandle();
            metadata.getPermissions().add("rest-reader", DocumentMetadataHandle.Capability.READ,
                DocumentMetadataHandle.Capability.EXECUTE);
            metadata.getPermissions().add("rest-writer", DocumentMetadataHandle.Capability.UPDATE);
            metadata.getMetadataValues().add("my-uri", document.getFileName());
            metadata.getProperties().put(new QName("org:example", "hello"), "world");
            metadata.withCollections(TEST_COLLECTION, TEST_COLLECTION + "-2");
            metadata.setQuality(12);
            writeSet.add(document.getFileName(), metadata, new StringHandle(document.getContent()));
        }
        mgr.write(writeSet);
    }

    @Test
    void test() {
        DatabaseClient client = getDatabaseClient();
        DataMovementManager dmm = client.newDataMovementManager();
        StructuredQueryDefinition query = client.newQueryManager().newStructuredQueryBuilder().collection(TEST_COLLECTION);
        QueryBatcher qb = dmm.newQueryBatcher(query);
        qb.withBatchSize(10);
        qb.withThreadCount(2);
        qb.onUrisReady(batch -> {
            // So really, we need a try/catch in the success listener
            logger.info("ITEMS: " + Arrays.asList(batch.getItems()));
            if (batch.getJobBatchNumber() == 2) {
                throw new RuntimeException("Boom! Batch 2");
            }
        });
        qb.onQueryFailure(failure -> {
            logger.error("FAILURE: " + failure.getMessage());
        });
        dmm.startJob(qb);
        qb.awaitCompletion();
        dmm.stopJob(qb);
        logger.info("DONE!");
    }

    @Override
    protected TestRunner getNewTestRunner(Class processor) {
        TestRunner runner = super.getNewTestRunner(processor);
        runner.assertNotValid();
        runner.setProperty(QueryMarkLogic.CONSISTENT_SNAPSHOT, "true");
        return runner;
    }

    @Test
    public void testSimpleCollectionQuery() {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, TEST_COLLECTION);
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COLLECTION);
        runner.enqueue(new MockFlowFile(12345));
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(QueryMarkLogic.ORIGINAL, 1);
        MockFlowFile originalFlowFile = runner.getFlowFilesForRelationship(QueryMarkLogic.ORIGINAL).get(0);
        assertEquals("If a FlowFile is passed to DeleteML/QueryML, it is expected to be sent to the " +
            "ORIGINAL relationship before the job completes", 12345, originalFlowFile.getId());

        runner.assertTransferCount(QueryMarkLogic.SUCCESS, numDocs);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS, CoreAttributes.FILENAME.key());

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        byte[] actualByteArray = null;
        for (MockFlowFile flowFile : flowFiles) {
            if (flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("/" + Integer.toString(jsonMod) + ".json")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(jsonMod).getContent().getBytes();
        assertEquals(expectedByteArray.length, actualByteArray.length);
        assertTrue(Arrays.equals(expectedByteArray, actualByteArray));
    }

    @Test
    public void testOldCollectionQuery() {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.COLLECTIONS, TEST_COLLECTION);
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, numDocs);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS, CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        byte[] actualByteArray = null;
        for (MockFlowFile flowFile : flowFiles) {
            if (flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("/" + Integer.toString(jsonMod) + ".json")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(jsonMod).getContent().getBytes();
        assertEquals(expectedByteArray.length, actualByteArray.length);
        assertTrue(Arrays.equals(expectedByteArray, actualByteArray));
    }

    /**
     * Despite the name, "Combined Query" supports a serialized CTS JSON query as well.
     */
    @Test
    public void testJSONSerializedCTSQuery() {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "{\n" +
            "  \"ctsquery\": {\n" +
            "    \"jsonPropertyValueQuery\": {\n" +
            "      \"property\": [\n" +
            "        \"sample\"\n" +
            "      ],\n" +
            "      \"value\": [\n" +
            "        \"jsoncontent\"\n" +
            "      ],\n" +
            "      \"options\": [\n" +
            "        \"lang=en\"\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COMBINED_JSON);

        verifyCombinedJSONQueryResults(runner);
    }

    @Test
    public void testCombinedJSONQuery() {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "{\"search\" : {\n" +
            "  \"ctsquery\": {\n" +
            "    \"jsonPropertyValueQuery\":{\n" +
            "      \"property\":[\"sample\"],\n" +
            "      \"value\":[\"jsoncontent\"]\n" +
            "      } \n" +
            "  }\n" +
            "} }");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COMBINED_JSON);

        verifyCombinedJSONQueryResults(runner);
    }

    @Test
    public void testStateManagerWithJSONCombinedQuery() throws IOException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "{\"search\" : {\n" +
            "  \"ctsquery\": {\n" +
            "    \"jsonPropertyValueQuery\":{\n" +
            "      \"property\":[\"sample\"],\n" +
            "      \"value\":[\"jsoncontent\"]\n" +
            "      } \n" +
            "  }\n" +
            "} }");
        runner.setProperty(QueryMarkLogic.RETURN_TYPE, QueryMarkLogic.ReturnTypes.DOCUMENTS_AND_META);
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COMBINED_JSON);
        testStateManagerJSON(runner, expectedJsonCount);
        runner.shutdown();
    }

    /**
     * After upgrading to Java Client 5.3.2 from 4.1.1, this is also causing a segfault on ML 10.0-5. It appears to be
     * due to bug 1283 as well.
     *
     * @throws InitializationException
     * @throws IOException
     */
    @Test
    @Disabled(value = "Disabled due to https://github.com/marklogic/java-client-api/issues/1283")
    public void testStateManagerWithXMLSerializedCTSQuery() throws IOException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "<cts:element-value-query xmlns:cts=\"http://marklogic.com/cts\">\n" +
            "  <cts:element>sample</cts:element>\n" +
            "  <cts:text xml:lang=\"en\">xmlcontent</cts:text>\n" +
            "</cts:element-value-query>");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COMBINED_XML);
        testStateManagerXML(runner, expectedXmlCount);
        runner.shutdown();
    }

    @Test
    public void testStateManagerWithJSONStructuredQuery() throws IOException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "{\n" +
            "  \"query\": {\n" +
            "    \"queries\": [\n" +
            "      { \n" +
            "       \"value-query\": {\n" +
            "          \"type\": \"string\",\n" +
            "          \"json-property\": [\"sample\"],\n" +
            "          \"text\": [\"jsoncontent\"]\n" +
            "        }" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRUCTURED_JSON);
        testStateManagerJSON(runner, expectedJsonCount);
        runner.shutdown();
    }

    @Test
    @Disabled(value = "Disabled due to https://github.com/marklogic/java-client-api/issues/1283")
    public void testStateManagerWithXMLStructuredQuery() throws IOException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "<query xmlns=\"http://marklogic.com/appservices/search\">\n" +
            "  <word-query>\n" +
            "    <element name=\"sample\" ns=\"\" />\n" +
            "    <text>xmlcontent</text>\n" +
            "  </word-query>\n" +
            "</query>");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRUCTURED_XML);
        testStateManagerXML(runner, expectedXmlCount);
        runner.shutdown();
    }

    @Test
    public void testStateManagerWithJSONStringQuery() throws IOException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "jsoncontent");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRING);
        testStateManagerJSON(runner, expectedJsonCount);
        runner.shutdown();
    }

    @Test
    @Disabled(value = "Disabled due to https://github.com/marklogic/java-client-api/issues/1283")
    public void testStateManagerWithXMLStringQuery() throws IOException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "xmlcontent");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRING);
        testStateManagerXML(runner, expectedXmlCount);
        runner.shutdown();
    }

    /**
     * Despite the name, "Combined Query" supports a serialized CTS XML query as well.
     */
    @Test
    public void testXMLSerializedCTSQuery() {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "<cts:element-value-query xmlns:cts=\"http://marklogic.com/cts\">\n" +
            "  <cts:element>sample</cts:element>\n" +
            "  <cts:text xml:lang=\"en\">xmlcontent</cts:text>\n" +
            "</cts:element-value-query>");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COMBINED_XML);

        verifyCombinedXMLQueryResults(runner);
    }

    @Test
    void testCombinedXMLQuery() {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        String combinedQuery = "<search:search xmlns:search=\"http://marklogic.com/appservices/search\">\n" +
            "  <search:query>\n" +
            "    <search:word-query>\n" +
            "      <search:element name=\"sample\"/>\n" +
            "      <search:text>xmlcontent</search:text>\n" +
            "    </search:word-query>" +
            "  </search:query>" +
            "</search:search>";
        runner.setProperty(QueryMarkLogic.QUERY, combinedQuery);
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COMBINED_XML);

        verifyCombinedXMLQueryResults(runner);
    }

    @Test
    public void testStructuredJSONQuery() {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "{\n" +
            "  \"query\": {\n" +
            "    \"queries\": [\n" +
            "      { \n" +
            "       \"value-query\": {\n" +
            "          \"type\": \"string\",\n" +
            "          \"json-property\": [\"sample\"],\n" +
            "          \"text\": [\"jsoncontent\"]\n" +
            "        }" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRUCTURED_JSON);
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedJsonCount);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS, CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        assertEquals(flowFiles.size(), expectedJsonCount);
        byte[] actualByteArray = null;
        for (MockFlowFile flowFile : flowFiles) {
            if (flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("/" + Integer.toString(jsonMod) + ".json")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(jsonMod).getContent().getBytes();
        assertEquals(expectedByteArray.length, actualByteArray.length);
        assertTrue(Arrays.equals(expectedByteArray, actualByteArray));
        runner.shutdown();
    }

    @Test
    public void testStructuredXMLQuery() {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("word", "xmlcontent");
        runner.enqueue("".getBytes(), attributes);
        runner.setProperty(QueryMarkLogic.QUERY, "<query xmlns=\"http://marklogic.com/appservices/search\">\n" +
            "  <word-query>\n" +
            "    <element name=\"sample\" ns=\"\" />\n" +
            "    <text>${word}</text>\n" +
            "  </word-query>\n" +
            "</query>");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRUCTURED_XML);
        runner.assertValid();
        runner.run();

        // The single enqueued doc should show up here
        runner.assertTransferCount(QueryMarkLogic.ORIGINAL, 1);

        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedXmlCount);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS, CoreAttributes.FILENAME.key());

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        assertEquals(flowFiles.size(), expectedXmlCount);
        byte[] actualByteArray = null;
        for (MockFlowFile flowFile : flowFiles) {
            if (flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("/" + Integer.toString(xmlMod) + ".xml")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(xmlMod).getContent().getBytes();

        assertBytesAreEqualXMLDocs(expectedByteArray, actualByteArray);
        runner.shutdown();
    }

    @Test
    public void testStringQuery() {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "xmlcontent");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRING);
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedXmlCount);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS, CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        assertEquals(flowFiles.size(), expectedXmlCount);
        byte[] actualByteArray = null;
        for (MockFlowFile flowFile : flowFiles) {
            if (flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("/" + Integer.toString(xmlMod) + ".xml")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(xmlMod).getContent().getBytes();

        assertBytesAreEqualXMLDocs(expectedByteArray, actualByteArray);
        runner.shutdown();
    }

    @Test
    public void testUrisOnly() {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.RETURN_TYPE, QueryMarkLogic.ReturnTypes.URIS_ONLY);
        runner.setProperty(QueryMarkLogic.QUERY, "xmlcontent");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRING);
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedXmlCount);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS, CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        assertEquals(flowFiles.size(), expectedXmlCount);
        for (MockFlowFile flowFile : flowFiles) {
            byte[] actualByteArray = runner.getContentAsByteArray(flowFile);
            assertEquals("Content should be empty since we only asked for URIs", actualByteArray.length, 0);
            String uri = flowFile.getAttribute("filename");
            assertTrue("Unexpected URI: " + uri, uri.startsWith("/PutMarkLogicTest/") && uri.endsWith(".xml"));
        }
        runner.shutdown();
    }

    @Test
    public void documentAndMetadataWithNoMetadata() {
        final String uri = "/QueryMarkLogicIT/123.json";
        final String uniqueString = "thisShouldOnlyAppearHere";
        try {
            ObjectNode content = new ObjectMapper().createObjectNode().put("hello", uniqueString);
            getDatabaseClient().newJSONDocumentManager().write(uri, new DocumentMetadataHandle(), new JacksonHandle(content));

            TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
            runner.setProperty(QueryMarkLogic.RETURN_TYPE, QueryMarkLogic.ReturnTypes.META);
            runner.setProperty(QueryMarkLogic.QUERY, uniqueString);
            runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRING);
            runner.assertValid();
            runner.run();

            List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
            assertEquals(1, flowFiles.size());
            MockFlowFile flowFile = flowFiles.get(0);
            System.out.println(flowFile.getAttributes());
            assertEquals("", flowFile.getAttribute("marklogic-collections"));
            assertEquals("", flowFile.getAttribute("marklogic-permissions"));
            assertEquals("0", flowFile.getAttribute("marklogic-quality"));

            Set<String> attributeNames = flowFile.getAttributes().keySet();
            for (String name : attributeNames) {
                assertFalse("No meta: attributes should exist since the document doesn't have any metadata values",
                    name.startsWith("meta:"));
                assertFalse("No property: attributes should exist since the document doesn't have any properties",
                    name.startsWith("property:"));
            }
        } finally {
            getDatabaseClient().newJSONDocumentManager().delete(uri);
        }
    }

    @Test
    public void onlyMetadata() {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.RETURN_TYPE, QueryMarkLogic.ReturnTypes.META);
        runner.setProperty(QueryMarkLogic.QUERY, "xmlcontent");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRING);
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedXmlCount);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS, CoreAttributes.FILENAME.key());

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        assertEquals(flowFiles.size(), expectedXmlCount);
        for (MockFlowFile flowFile : flowFiles) {
            byte[] actualByteArray = runner.getContentAsByteArray(flowFile);
            assertEquals("Content should be empty since we only asked for metadata", actualByteArray.length, 0);

            // Verify metadata attributes exist
            assertEquals("world", flowFile.getAttribute("property:{org:example}hello"));
            String uri = flowFile.getAttribute("filename");
            assertEquals(uri, flowFile.getAttribute("meta:my-uri"));

            String permissions = flowFile.getAttribute("marklogic-permissions");
            System.out.println(permissions);
            assertTrue(permissions.contains("rest-reader,read"));
            assertTrue(permissions.contains("rest-reader,execute"));
            assertTrue(permissions.contains("rest-writer,update"));

            List<String> collections = Arrays.asList(flowFile.getAttribute("marklogic-collections").split(","));
            assertTrue(collections.contains(TEST_COLLECTION));
            assertTrue(collections.contains(TEST_COLLECTION + "-2"));

            assertEquals("12", flowFile.getAttribute("marklogic-quality"));
        }
        runner.shutdown();
    }

    @Test
    public void testJobProperties() {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, TEST_COLLECTION);
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COLLECTION);
        runner.run();
        Processor processor = runner.getProcessor();
        if (processor instanceof QueryMarkLogic) {
            QueryBatcher queryBatcher = ((QueryMarkLogic) processor).getQueryBatcher();
            assertEquals(Integer.parseInt(batchSize), queryBatcher.getBatchSize());
            assertEquals(Integer.parseInt(threadCount), queryBatcher.getThreadCount());
        } else {
            fail("Processor not an instance of QueryMarkLogic");
        }
        runner.shutdown();
    }

    private void assertBytesAreEqualXMLDocs(byte[] expectedByteArray, byte[] actualByteArray) {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            dbf.setCoalescing(true);
            dbf.setIgnoringElementContentWhitespace(true);
            dbf.setIgnoringComments(true);
            DocumentBuilder db = dbf.newDocumentBuilder();

            Document doc1 = db.parse(new ByteArrayInputStream(actualByteArray));
            doc1.normalizeDocument();

            Document doc2 = db.parse(new ByteArrayInputStream(expectedByteArray));
            doc2.normalizeDocument();

            assertTrue(doc1.isEqualNode(doc2));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void testStateManagerJSON(TestRunner runner, int expectedCount) throws IOException {
        runner.setProperty(QueryMarkLogic.STATE_INDEX, "dateTime");
        runner.setProperty(QueryMarkLogic.STATE_INDEX_TYPE, QueryMarkLogic.IndexTypes.JSON_PROPERTY);
        testStateManagerWithIndex(runner, expectedCount);
    }

    private void testStateManagerXML(TestRunner runner, int expectedCount) throws IOException {
        runner.setProperty("ns:nst", "namespace-test");
        runner.setProperty(QueryMarkLogic.STATE_INDEX, "/root/nst:dateTime");
        runner.setProperty(QueryMarkLogic.STATE_INDEX_TYPE, QueryMarkLogic.IndexTypes.PATH);
        testStateManagerWithIndex(runner, expectedCount);
        runner.setProperty("ns:t", "namespace-test");
        runner.setProperty(QueryMarkLogic.STATE_INDEX, "t:dateTime");
        runner.setProperty(QueryMarkLogic.STATE_INDEX_TYPE, QueryMarkLogic.IndexTypes.ELEMENT);
        testStateManagerWithIndex(runner, expectedCount);
    }

    private void testStateManagerWithIndex(TestRunner runner, int expectedCount) throws IOException {
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedCount);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS, CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        assertEquals(flowFiles.size(), expectedCount);
        runner.clearTransferState();
        runner.getStateManager().assertStateEquals("queryState", "2000-01-01T00:00:00", Scope.CLUSTER);
        runner.run();
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, 0);
        runner.clearTransferState();
        HashMap<String, String> state = new HashMap<String, String>();
        state.put("queryState", "1999-01-01T00:00:00");
        runner.getStateManager().setState(state, Scope.CLUSTER);
        runner.run();
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedCount);
        runner.clearTransferState();
        runner.getStateManager().setState(new HashMap<>(), Scope.CLUSTER);
    }

    private void verifyCombinedXMLQueryResults(TestRunner runner) {
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedXmlCount);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS, CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        assertEquals(flowFiles.size(), expectedXmlCount);
        byte[] actualByteArray = null;
        for (MockFlowFile flowFile : flowFiles) {
            if (flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("/" + Integer.toString(xmlMod) + ".xml")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(xmlMod).getContent().getBytes();

        assertBytesAreEqualXMLDocs(expectedByteArray, actualByteArray);
        runner.shutdown();
    }

    private void verifyCombinedJSONQueryResults(TestRunner runner) {
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedJsonCount);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS, CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        assertEquals(flowFiles.size(), expectedJsonCount);
        byte[] actualByteArray = null;
        for (MockFlowFile flowFile : flowFiles) {
            if (flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("/" + jsonMod + ".json")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(jsonMod).getContent().getBytes();
        assertEquals(expectedByteArray.length, actualByteArray.length);
        assertTrue(Arrays.equals(expectedByteArray, actualByteArray));
        runner.shutdown();
    }
}
