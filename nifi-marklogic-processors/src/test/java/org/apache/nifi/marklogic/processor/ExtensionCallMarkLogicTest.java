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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.io.Format;
import org.apache.nifi.marklogic.processor.ExecuteScriptMarkLogicTest.TestExecuteScriptMarkLogic;
import org.apache.nifi.marklogic.processor.ExtensionCallMarkLogic.MethodTypes;
import org.apache.nifi.marklogic.processor.ExtensionCallMarkLogic.PayloadSources;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.junit.Before;
import org.junit.Test;

public class ExtensionCallMarkLogicTest extends AbstractMarkLogicProcessorTest {
    private TestExtensionCallMarkLogic processor;

    @Before
    public void setup() throws InitializationException {
        processor = new TestExtensionCallMarkLogic();
        initialize(processor);
        runner.setProperty(TestExecuteScriptMarkLogic.DATABASE_CLIENT_SERVICE, databaseClientServiceIdentifier);
    }

    @Test
    public void testValidator() throws Exception {
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.assertNotValid();

        runner.setProperty(TestExtensionCallMarkLogic.EXTENSION_NAME, "extension");
        runner.setProperty(TestExtensionCallMarkLogic.REQUIRES_INPUT, "true");
        runner.setProperty(TestExtensionCallMarkLogic.PAYLOAD_SOURCE, PayloadSources.NONE);
        runner.setProperty(TestExtensionCallMarkLogic.PAYLOAD_FORMAT, Format.TEXT.name());
        runner.setProperty(TestExtensionCallMarkLogic.METHOD_TYPE, MethodTypes.POST);
        runner.assertValid();
    }

    @Test
    public void putExtensionParams() {
        runner.enableControllerService(service);

        runner.setProperty(TestExtensionCallMarkLogic.EXTENSION_NAME, "example-rest");
        runner.setProperty(TestExtensionCallMarkLogic.REQUIRES_INPUT, "false");
        runner.setProperty(TestExtensionCallMarkLogic.PAYLOAD_SOURCE, PayloadSources.FLOWFILE_CONTENT_STR);
        runner.setProperty(TestExtensionCallMarkLogic.PAYLOAD_FORMAT, Format.TEXT.name());
        runner.setProperty(TestExtensionCallMarkLogic.METHOD_TYPE, MethodTypes.PUT_STR);

        runner.setProperty("param:uri", "value");
        processor.initialize(initializationContext);

        processor.onTrigger(runner.getProcessContext(), mockProcessSessionFactory);


        assertEquals("value", runner.getProcessContext().getProperty("param:uri").getValue());
        assertEquals(MethodTypes.PUT_STR, runner.getProcessContext().getProperty(TestExtensionCallMarkLogic.METHOD_TYPE).toString());


        assertEquals(2, processor.relationships.size());


    }

    class TestExtensionCallMarkLogic extends ExtensionCallMarkLogic {
        @Override
        public DatabaseClient getDatabaseClient(ProcessContext context) {
            return new TestMLDatabaseClient();
        }
    }
}
