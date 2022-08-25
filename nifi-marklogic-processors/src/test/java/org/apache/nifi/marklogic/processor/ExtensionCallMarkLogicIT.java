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

import com.marklogic.client.io.Format;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Deprecated
public class ExtensionCallMarkLogicIT extends AbstractMarkLogicIT {

    @Test
    void simpleGet() {
        TestRunner runner = super.getNewTestRunner(ExtensionCallMarkLogic.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExtensionCallMarkLogic.EXTENSION_NAME, "replay");
        runner.setProperty(ExtensionCallMarkLogic.METHOD_TYPE, ExtensionCallMarkLogic.MethodTypes.GET_STR);
        runner.setProperty(ExtensionCallMarkLogic.REQUIRES_INPUT, "true");
        runner.setProperty(ExtensionCallMarkLogic.PAYLOAD_SOURCE, ExtensionCallMarkLogic.PayloadSources.NONE);
        runner.setProperty(ExtensionCallMarkLogic.PAYLOAD_FORMAT, Format.TEXT.name());
        runner.setProperty("param:replay", "${dynamicParam}");

        runner.run();
        assertEquals(0, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.FAILURE).size());
        assertEquals(0, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.SUCCESS).size(),
                "Expecting no FlowFiles; the processor should not have run since requiresInput=true and no " +
                        "FlowFile was enqueued");

        MockFlowFile mockFlowFile = new MockFlowFile(1);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("dynamicParam", "dynamicValue");
        mockFlowFile.putAttributes(attributes);

        runner.enqueue(mockFlowFile);
        runner.run(1);

        runner.assertQueueEmpty();
        assertEquals(0, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.FAILURE).size());
        assertEquals(1, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.SUCCESS).size());

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.SUCCESS);
        assertEquals(1, results.size());

        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));
        assertEquals("dynamicValue", resultValue,
                "The test 'replay' extension is expected to return the value of the 'replay' parameter, " +
                        "which is sent via the replay:param property. That property then has an expression, " +
                        "which is expected to be evaluated against the flowfile attributes, producing the " +
                        "value 'dynamicvalue'");
    }

    @Test
    void simplePost() {
        TestRunner runner = getNewTestRunner(ExtensionCallMarkLogic.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExtensionCallMarkLogic.EXTENSION_NAME, "replay");
        runner.setProperty(ExtensionCallMarkLogic.METHOD_TYPE, ExtensionCallMarkLogic.MethodTypes.POST_STR);
        runner.setProperty(ExtensionCallMarkLogic.REQUIRES_INPUT, "true");
        runner.setProperty(ExtensionCallMarkLogic.PAYLOAD_SOURCE, ExtensionCallMarkLogic.PayloadSources.FLOWFILE_CONTENT_STR);
        runner.setProperty(ExtensionCallMarkLogic.PAYLOAD_FORMAT, Format.TEXT.name());

        Map<String, String> attributesMap = new HashMap<>();
        String testString = "checkForThisPost";

        runner.enqueue(testString, attributesMap);
        runner.run(1);
        runner.assertQueueEmpty();
        assertEquals(0, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.FAILURE).size());
        assertEquals(1, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.SUCCESS).size());

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.SUCCESS);
        assertEquals(1, results.size());
        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));

        assertEquals(testString + testString, resultValue);
    }

    @Test
    void resourceReturns500() {
        TestRunner runner = getNewTestRunner(ExtensionCallMarkLogic.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExtensionCallMarkLogic.EXTENSION_NAME, "throwsError");
        runner.setProperty(ExtensionCallMarkLogic.METHOD_TYPE, ExtensionCallMarkLogic.MethodTypes.GET_STR);
        runner.setProperty(ExtensionCallMarkLogic.REQUIRES_INPUT, "false");
        runner.setProperty(ExtensionCallMarkLogic.PAYLOAD_SOURCE, ExtensionCallMarkLogic.PayloadSources.NONE);

        runner.enqueue();
        runner.run();
        runner.assertQueueEmpty();

        assertEquals(1, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.FAILURE).size(),
                "One FlowFile should exist due to the REST extension throwing an error; the FlowFile should " +
                        "have been created since requiresInput=false");
        assertEquals(0, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.SUCCESS).size());

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.FAILURE).get(0);
        assertTrue(flowFile.getAttributes().containsKey("markLogicErrorMessage"), "The root cause error message " +
                "should be added to the flowfile attributes so that a downstream processor can easily access it");
        String message = flowFile.getAttributes().get("markLogicErrorMessage");
        assertTrue(message.contains("Server Message: GetError Description (GetErrorQName): 500 GetError message"),
                "The error message should contain the error details from the REST extension");
    }

    @Test
    void getEmptyPayload() {
        TestRunner runner = newEmptyEndpointsTestRunner(ExtensionCallMarkLogic.MethodTypes.GET_STR);
        runWithSingleEmptyFlowFile(runner);
        assertEquals(1, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.SUCCESS).size());
    }

    @Test
    void putEmptyPayload() {
        TestRunner runner = newEmptyEndpointsTestRunner(ExtensionCallMarkLogic.MethodTypes.PUT_STR);
        runWithSingleEmptyFlowFile(runner);
        assertEquals(1, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.SUCCESS).size());
    }

    @Test
    void putPayloadWithEmptyByteArray() {
        TestRunner runner = newEmptyEndpointsTestRunner(ExtensionCallMarkLogic.MethodTypes.PUT_STR);

        runner.enqueue(new byte[]{});
        runner.run(1);
        runner.assertQueueEmpty();

        assertEquals(1, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.SUCCESS).size(),
                "Verifies that an empty byte array is fine; the Java Client only complains if the byte array is null");
    }

    @Test
    void postEmptyPayload() {
        TestRunner runner = newEmptyEndpointsTestRunner(ExtensionCallMarkLogic.MethodTypes.POST_STR);
        runWithSingleEmptyFlowFile(runner);
        assertEquals(1, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.SUCCESS).size());
    }

    @Test
    void deleteEmptyPayload() {
        TestRunner runner = newEmptyEndpointsTestRunner(ExtensionCallMarkLogic.MethodTypes.DELETE_STR);
        runWithSingleEmptyFlowFile(runner);
        assertEquals(1, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.SUCCESS).size());
    }

    private TestRunner newEmptyEndpointsTestRunner(String methodType) {
        TestRunner runner = getNewTestRunner(ExtensionCallMarkLogic.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExtensionCallMarkLogic.EXTENSION_NAME, "emptyEndpoints");
        runner.setProperty(ExtensionCallMarkLogic.METHOD_TYPE, methodType);
        runner.setProperty(ExtensionCallMarkLogic.REQUIRES_INPUT, "true");
        runner.setProperty(ExtensionCallMarkLogic.PAYLOAD_SOURCE, ExtensionCallMarkLogic.PayloadSources.NONE);
        return runner;
    }

    private void runWithSingleEmptyFlowFile(TestRunner runner) {
        runner.enqueue(new MockFlowFile(1));
        runner.run(1);
        runner.assertQueueEmpty();
    }
}
