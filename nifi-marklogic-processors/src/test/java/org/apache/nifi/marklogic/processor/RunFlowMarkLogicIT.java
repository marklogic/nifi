package org.apache.nifi.marklogic.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RunFlowMarkLogicIT extends AbstractMarkLogicIT {

    @Test
    void realFlow() {
        final String uri = "/a1.json";

        getDatabaseClient().newJSONDocumentManager().write(uri,
            new DocumentMetadataHandle()
                .withPermission("rest-reader", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE)
                .withCollections("staging-data"),
            new StringHandle("{}"));

        TestRunner runner = getNewTestRunner(RunFlowMarkLogic.class);
        runner.setProperty(RunFlowMarkLogic.FLOW_NAME, "testFlow");
        runner.run();

        JsonNode doc = getDatabaseClient().newJSONDocumentManager().read(uri, new JacksonHandle()).get();
        assertTrue(doc.has("envelope"), "The custom step in the testFlow flow should have wrapped the document in an envelope.");
    }
    
    @Test
    void flowDoesntExist() {
        TestRunner runner = newReaderTestRunner(RunFlowMarkLogic.class);
        runner.setProperty(RunFlowMarkLogic.FLOW_NAME, "doesntExist");
        runner.run();

        assertEquals(0, runner.getFlowFilesForRelationship(RunFlowMarkLogic.FINISHED).size());

        List<MockFlowFile> files = runner.getFlowFilesForRelationship(RunFlowMarkLogic.FAILURE);
        assertEquals(1, files.size());
        String errorMessage = files.get(0).getAttribute("markLogicErrorMessage");
        assertTrue(errorMessage.startsWith("Unable to retrieve flow with name: doesntExist"),
            "Expecting the processor to fail because the flow can't be found, since the test doesn't even run " +
                "against a DHF application; error message: " + errorMessage);
    }

}
