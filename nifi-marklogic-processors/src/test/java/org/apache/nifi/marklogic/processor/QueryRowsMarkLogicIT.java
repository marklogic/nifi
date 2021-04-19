package org.apache.nifi.marklogic.processor;

import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryRowsMarkLogicIT extends AbstractMarkLogicIT {

    @BeforeEach
    public void setup() {
        JSONDocumentManager mgr = getDatabaseClient().newJSONDocumentManager();
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        metadata.withCollections("example");
        for (int i = 0; i < 5; i++) {
            mgr.write("/example/" + i + ".json", metadata, new StringHandle(format("{\"Id\":\"%d\"}", i)));
        }
    }

    @Test
    public void test() {
        // Generated via require('/MarkLogic/optic').fromView("Example", "default").orderBy("Id").export()
        final String serializedPlan = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Example\", \"default\", null, null]}, {\"ns\":\"op\", \"fn\":\"order-by\", \"args\":[[{\"ns\":\"op\", \"fn\":\"col\", \"args\":[\"Id\"]}]]}]}}";

        TestRunner runner = getNewTestRunner(QueryRowsMarkLogic.class);
        runner.setProperty(QueryRowsMarkLogic.PLAN, serializedPlan);
        runner.run();

        List<MockFlowFile> list = runner.getFlowFilesForRelationship(QueryRowsMarkLogic.SUCCESS);
        assertEquals(new Integer(1), list.size(), "A single FlowFile with the TDE response should have been transferred");

        // Newline characters are making it difficult to assert on the entire response at once
        final String response = new String(list.get(0).toByteArray());
        assertTrue(response.startsWith("Example.default.Id"));
        assertTrue(response.contains("0"));
        assertTrue(response.contains("1"));
        assertTrue(response.contains("2"));
        assertTrue(response.contains("3"));
        assertTrue(response.contains("4"));
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

        TestRunner runner = getNewTestRunner(QueryRowsMarkLogic.class);
        runner.setProperty(QueryRowsMarkLogic.PLAN, serializedPlan);
        runner.run();

        List<MockFlowFile> list = runner.getFlowFilesForRelationship(QueryRowsMarkLogic.SUCCESS);
        assertEquals(new Integer(1), list.size(), "A single FlowFile with the TDE response should have been transferred");

        final String response = new String(list.get(0).toByteArray());
        assertEquals("", response, "The response should be empty since the 'where' clause resulted in no rows being " +
                "found; no error should be thrown either, as the serialized plan is valid; it just doesn't match anything");
    }
}
