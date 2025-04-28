package org.apache.nifi.marklogic.processor;

import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryRowsMarkLogicIT extends AbstractMarkLogicIT {

    @BeforeEach
    public void setup() {
        JSONDocumentManager mgr = getDatabaseClient().newJSONDocumentManager();
        DocumentMetadataHandle metadata = new DocumentMetadataHandle()
            .withPermission("rest-reader", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE);
        metadata.withCollections("example");
        for (int i = 0; i < 5; i++) {
            mgr.write("/example/" + i + ".json", metadata, new StringHandle(format("{\"Id\":\"%d\"}", i)));
        }
    }

    @Test
    public void testWithNoIncomingFlowFile() {
        // Generated via require('/MarkLogic/optic').fromView("Example","default").orderBy("Id").export()
        final String serializedPlan = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Example\", \"default\", null, null]}, {\"ns\":\"op\", \"fn\":\"order-by\", \"args\":[[{\"ns\":\"op\", \"fn\":\"col\", \"args\":[\"Id\"]}]]}]}}";

        TestRunner runner = newReaderTestRunner(QueryRowsMarkLogic.class);
        runner.setProperty(QueryRowsMarkLogic.PLAN, serializedPlan);
        runner.run();

        List<MockFlowFile> files = runner.getFlowFilesForRelationship(QueryRowsMarkLogic.ORIGINAL);
        assertEquals(1, files.size(), "A new FlowFile should have been created and sent to Original");
        assertEquals(serializedPlan, files.get(0).getAttribute("marklogic-optic-plan"),
            "The serialized plan should be" +
            " on the success FlowFile so that a user knows what was executed to produce the rows");

        files = runner.getFlowFilesForRelationship(QueryRowsMarkLogic.SUCCESS);
        assertEquals(1, files.size(), "A single FlowFile with the TDE response should have been transferred");

        // Newline characters are making it difficult to assert on the entire response at once
        final String response = new String(files.getFirst().toByteArray(),StandardCharsets.UTF_8);
        assertTrue(response.startsWith("Example.default.Id"));
        assertTrue(response.contains("0"));
        assertTrue(response.contains("1"));
        assertTrue(response.contains("2"));
        assertTrue(response.contains("3"));
        assertTrue(response.contains("4"));

        assertEquals(0, runner.getFlowFilesForRelationship(QueryRowsMarkLogic.FAILURE).size());
    }

    @Test
    public void testWithIncomingFlowFile() {
        final String serializedPlan = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Example\", \"default\", null, null]}, {\"ns\":\"op\", \"fn\":\"order-by\", \"args\":[[{\"ns\":\"op\", \"fn\":\"col\", \"args\":[\"Id\"]}]]}]}}";

        TestRunner runner = newReaderTestRunner(QueryRowsMarkLogic.class);
        runner.setProperty(QueryRowsMarkLogic.PLAN, serializedPlan);

        MockFlowFile mockFlowFile = new MockFlowFile(System.currentTimeMillis());
        Map<String, String> attributes = new HashMap<>();
        attributes.put("test-param1", "value1");
        mockFlowFile.putAttributes(attributes);
        runner.enqueue(mockFlowFile);

        runner.run();

        List<MockFlowFile> files = runner.getFlowFilesForRelationship(QueryRowsMarkLogic.ORIGINAL);
        assertEquals(1, files.size(), "A new FlowFile should have been created and sent to Original");
        MockFlowFile originalFile = files.get(0);
        assertEquals("value1", originalFile.getAttribute("test-param1"), "Expecting the enqueued FlowFile to be sent " +
            "to Original, and thus it should have the test-param1 attribute");
        assertEquals(serializedPlan, originalFile.getAttribute("marklogic-optic-plan"),
            "The serialized plan should be on the success FlowFile so that a user knows what was executed to produce the rows");

        files = runner.getFlowFilesForRelationship(QueryRowsMarkLogic.SUCCESS);
        assertEquals(1, files.size());
        MockFlowFile successFlowFile = files.get(0);
        assertEquals("value1", successFlowFile.getAttribute("test-param1"), "The success FlowFile should have been " +
            "created from the original FlowFile, so test-param1 should exist");

        assertEquals(0, runner.getFlowFilesForRelationship(QueryRowsMarkLogic.FAILURE).size());
    }

    @Test
    public void invalidPlan() {
        final String serializedPlan = "invalid Plan";
        final String expectedErrorMessage = "Local message: failed to apply resource at rows: Bad Request. Server Message: XDMP-JSONDOC: xdmp.getRequestBody(\"json\") -- Document is not JSON";
        TestRunner runner = newReaderTestRunner(QueryRowsMarkLogic.class);
        runner.setProperty(QueryRowsMarkLogic.PLAN, serializedPlan);

        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship(QueryRowsMarkLogic.FAILURE).size());

        List<MockFlowFile> files = runner.getFlowFilesForRelationship(QueryRowsMarkLogic.FAILURE);
        assertEquals(1, files.size(), "A new FlowFile should have been created and sent to Failure");
        MockFlowFile failureFile = files.get(0);
        assertEquals(expectedErrorMessage, failureFile.getAttribute("markLogicErrorMessage"),
            "The Failure FlowFile should contain the marklogicErrorMessage attribute.");

        assertEquals(0, runner.getFlowFilesForRelationship(QueryRowsMarkLogic.SUCCESS).size(),
            "No rows were returned, so no FlowFiles should have been sent to Success");

        assertEquals(0, runner.getFlowFilesForRelationship(QueryRowsMarkLogic.ORIGINAL).size());
    }

    @Test
    public void noRowsFound() {
        final String serializedPlan = "{\n" +
            "  \"$optic\": {\n" +
            "    \"ns\": \"op\",\n" +
            "    \"fn\": \"operators\",\n" +
            "    \"args\": [\n" +
            "      {\n" +
            "        \"ns\": \"op\",\n" +
            "        \"fn\": \"from-view\",\n" +
            "        \"args\": [\n" +
            "          \"Example\",\n" +
            "          \"default\",\n" +
            "          null,\n" +
            "          null\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"ns\": \"op\",\n" +
            "        \"fn\": \"where\",\n" +
            "        \"args\": [\n" +
            "          {\n" +
            "            \"ns\": \"op\",\n" +
            "            \"fn\": \"sql-condition\",\n" +
            "            \"args\": [\n" +
            "              \"Id > 5\"\n" +
            "            ]\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}";

        TestRunner runner = newReaderTestRunner(QueryRowsMarkLogic.class);
        runner.setProperty(QueryRowsMarkLogic.PLAN, serializedPlan);
        runner.run();

        List<MockFlowFile> list = runner.getFlowFilesForRelationship(QueryRowsMarkLogic.ORIGINAL);
        assertEquals(Integer.valueOf(1), list.size(), "The new incoming FlowFile should go to Original");
        assertEquals(serializedPlan, list.getFirst().getAttribute("marklogic-optic-plan"), "The serialized plan should be" +
            " on the success FlowFile so that a user knows what was executed to produce the rows");

        final String response = new String(list.getFirst().toByteArray(), StandardCharsets.UTF_8);
        assertEquals("", response, "The response should be empty since the 'where' clause resulted in no rows being " +
            "found; no error should be thrown either, as the serialized plan is valid; it just doesn't match anything");

        assertEquals(0, runner.getFlowFilesForRelationship(QueryRowsMarkLogic.SUCCESS).size(),
            "No rows were returned, so no FlowFiles should have been sent to Success");

        assertEquals(0, runner.getFlowFilesForRelationship(QueryRowsMarkLogic.FAILURE).size());
    }
}
