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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.junit.Before;
import org.junit.Test;

import com.marklogic.client.DatabaseClient;

public class ExecuteScriptMarkLogicTest extends AbstractMarkLogicProcessorTest {

    private TestExecuteScriptMarkLogic processor;

    @Before
    public void setup() throws InitializationException {
        processor = new TestExecuteScriptMarkLogic();
        initialize(processor);
        runner.setProperty(TestExecuteScriptMarkLogic.DATABASE_CLIENT_SERVICE, databaseClientServiceIdentifier);
    }

    @Test
    public void testValidator() throws Exception {
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.assertNotValid();

        runner.setProperty(TestExecuteScriptMarkLogic.EXECUTION_TYPE, TestExecuteScriptMarkLogic.AV_JAVASCRIPT);
        runner.setProperty(TestExecuteScriptMarkLogic.SCRIPT_BODY, "{}");
        runner.assertValid();

        runner.setProperty(TestExecuteScriptMarkLogic.MODULE_PATH, "/path/to/modules.sjs");
        runner.assertNotValid();

        runner.removeProperty(TestExecuteScriptMarkLogic.MODULE_PATH);
        runner.setProperty("myExternalVarible", "value");
        runner.assertValid();

        runner.enqueue("test file");
        runner.run();
        TestServerEvaluationCall serverEval = (TestServerEvaluationCall) processor.getDatabaseClient(processContext).newServerEval();
        assertEquals(1, serverEval.javascriptCalls);
        assertEquals(0, serverEval.xqueryCalls);
        assertEquals(0, serverEval.modulePathCalls);
        assertEquals("value", serverEval.variables.get("myExternalVarible"));
        serverEval.reset();
    }

    class TestExecuteScriptMarkLogic extends ExecuteScriptMarkLogic {
        TestMLDatabaseClient testClient = new TestMLDatabaseClient();
        @Override
        public DatabaseClient getDatabaseClient(ProcessContext context) {
            return testClient;
        }
    }
}
