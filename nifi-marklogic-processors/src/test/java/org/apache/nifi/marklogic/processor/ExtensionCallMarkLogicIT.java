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
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExtensionCallMarkLogicIT extends AbstractMarkLogicIT {

    @BeforeEach
    public void setup() {
        super.setup();
    }

    public TestRunner getNewTestRunner(Class processor) {
        TestRunner runner = super.getNewTestRunner(processor);
        runner.setProperty(ExtensionCallMarkLogic.EXTENSION_NAME, "replay");
        return runner;
    }

    @Test
    public void simpleGet() throws InitializationException {
        TestRunner runner = getNewTestRunner(ExtensionCallMarkLogic.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExtensionCallMarkLogic.METHOD_TYPE, ExtensionCallMarkLogic.MethodTypes.GET_STR);
        runner.setProperty(ExtensionCallMarkLogic.REQUIRES_INPUT, "true");
        runner.setProperty(ExtensionCallMarkLogic.PAYLOAD_SOURCE, ExtensionCallMarkLogic.PayloadSources.NONE);
        runner.setProperty(ExtensionCallMarkLogic.PAYLOAD_FORMAT, Format.TEXT.name());

        Map<String, String> attributesMap = new HashMap<>();
        String testString = "checkforthis";
        attributesMap.put("param:replay", testString);

        runner.enqueue("blah", attributesMap);
        runner.run(1);
        runner.assertQueueEmpty();
        assertEquals(0, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.FAILURE).size());
        assertEquals(1, runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.SUCCESS).size());

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ExtensionCallMarkLogic.SUCCESS);
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));

        assertEquals("blah", resultValue);
    }

    @Test
    public void simplePost() throws InitializationException {
        TestRunner runner = getNewTestRunner(ExtensionCallMarkLogic.class);
        runner.setValidateExpressionUsage(false);
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
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));

        assertEquals(testString + testString, resultValue);
    }

}
