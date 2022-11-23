package org.apache.nifi.marklogic.processor;

import com.marklogic.junit5.XmlNode;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CallRestExtensionMarkLogicIT extends AbstractMarkLogicIT {

    @Test
    void noIncomingFlowFile() {
        TestRunner runner = newReaderTestRunner(CallRestExtensionMarkLogic.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(CallRestExtensionMarkLogic.EXTENSION_NAME, "multipleItems");
        runner.setProperty(CallRestExtensionMarkLogic.METHOD_TYPE, CallRestExtensionMarkLogic.MethodTypes.GET_STR);

        runner.run();
        runner.assertQueueEmpty();

        assertEquals(0, runner.getFlowFilesForRelationship(CallRestExtensionMarkLogic.FAILURE).size());
        assertEquals(1, runner.getFlowFilesForRelationship(CallRestExtensionMarkLogic.ORIGINAL).size(),
            "The just-created FlowFile should be sent to Original, although a user likely won't care " +
                "about it");

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(CallRestExtensionMarkLogic.RESULTS);
        assertEquals(2, results.size());

        XmlNode firstDoc = new XmlNode(new String(runner.getContentAsByteArray(results.get(0))));
        assertEquals("doc1", firstDoc.getElementValue("/first"));

        XmlNode secondDoc = new XmlNode(new String(runner.getContentAsByteArray(results.get(1))));
        assertEquals("doc2", secondDoc.getElementValue("/second"));
    }

    @Test
    void withIncomingFlowFile() {
        TestRunner runner = newReaderTestRunner(CallRestExtensionMarkLogic.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(CallRestExtensionMarkLogic.EXTENSION_NAME, "multipleItems");
        runner.setProperty(CallRestExtensionMarkLogic.METHOD_TYPE, CallRestExtensionMarkLogic.MethodTypes.GET_STR);

        MockFlowFile mockFlowFile = new MockFlowFile(System.currentTimeMillis());
        Map<String, String> attributes = new HashMap<>();
        attributes.put("param1", "value1");
        attributes.put("param2", "value2");
        mockFlowFile.putAttributes(attributes);

        runner.enqueue(mockFlowFile);
        runner.run();
        runner.assertQueueEmpty();

        assertEquals(0, runner.getFlowFilesForRelationship(CallRestExtensionMarkLogic.FAILURE).size());

        List<MockFlowFile> originals = runner.getFlowFilesForRelationship(CallRestExtensionMarkLogic.ORIGINAL);
        assertEquals(1, originals.size());
        MockFlowFile original = originals.get(0);
        String message = "The attributes on the original FlowFile should still exist";
        assertEquals("value1", original.getAttribute("param1"), message);
        assertEquals("value2", original.getAttribute("param2"), message);

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(CallRestExtensionMarkLogic.RESULTS);
        assertEquals(2, results.size());

        message = "Each 'results' FlowFile should have the attributes from the original FlowFile";
        for (MockFlowFile flowFile : results) {
            assertEquals("value1", flowFile.getAttribute("param1"), message);
            assertEquals("value2", flowFile.getAttribute("param2"), message);
        }
    }

    @Test
    void callFails() {
        TestRunner runner = newReaderTestRunner(CallRestExtensionMarkLogic.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(CallRestExtensionMarkLogic.EXTENSION_NAME, "multipleItems");
        runner.setProperty(CallRestExtensionMarkLogic.METHOD_TYPE, CallRestExtensionMarkLogic.MethodTypes.POST_STR);

        runner.run();
        runner.assertQueueEmpty();

        assertEquals(0, runner.getFlowFilesForRelationship(CallRestExtensionMarkLogic.ORIGINAL).size());
        assertEquals(0, runner.getFlowFilesForRelationship(CallRestExtensionMarkLogic.RESULTS).size());

        List<MockFlowFile> failures = runner.getFlowFilesForRelationship(CallRestExtensionMarkLogic.FAILURE);
        assertEquals(1, failures.size());

        String error = failures.get(0).getAttribute("markLogicErrorMessage");
        assertTrue(error.contains("Undefined function post()"), "Unexpected error: " + error);
    }
}
