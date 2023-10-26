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
package org.apache.nifi.marklogic.controller;

import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.SecurityContextType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultMarkLogicDatabaseClientServiceTest {

    /**
     * This doesn't need valid credentials, as its scope is just ensuring that with a bare minimum set of
     * properties defined, the controller service is considered valid. And that when the service is disabled,
     * the DatabaseClient is nulled out.
     *
     * @throws InitializationException
     */
    @Test
    public void testService() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DefaultMarkLogicDatabaseClientService service = new DefaultMarkLogicDatabaseClientService();
        runner.addControllerService("test-good", service);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.HOST, "localhost");
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PORT, "8000");
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.USERNAME, "admin");
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PASSWORD, "doesntmatter");

        runner.enableControllerService(service);
        runner.assertValid(service);

        runner.disableControllerService(service);
        assertNull(service.getDatabaseClient(),
            "When the processor is disabled, the DatabaseClient should be released and set to null");
    }

    @Test
    public void cloudAuth() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DefaultMarkLogicDatabaseClientService service = new DefaultMarkLogicDatabaseClientService();
        runner.addControllerService("test-good", service);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.HOST, "localhost");
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PORT, "443");
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.CLOUD_API_KEY, "doesntmatter");
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.BASE_PATH, "/some/path");
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.SECURITY_CONTEXT_TYPE, "CLOUD");

        runner.assertValid(service);

        try {
            runner.enableControllerService(service);
        } catch (AssertionFailedError ex) {
            assertTrue(ex.getMessage().contains("Unable to call token endpoint at https://localhost/token"),
                "enableControllerService should fail because it tries to authenticate with MarkLogic Cloud, but the " +
                    "inputs aren't valid. Which is good - the goal of this is to verify that it attempts to use " +
                    "cloud authentication. Actual error: " + ex.getMessage());
        }

        DatabaseClientConfig config = service.getDatabaseClientConfig();
        assertEquals("localhost", config.getHost());
        assertEquals(443, config.getPort());
        assertEquals("doesntmatter", config.getCloudApiKey());
        assertEquals("/some/path", config.getBasePath());
        assertEquals(SecurityContextType.CLOUD, config.getSecurityContextType());
    }
}
