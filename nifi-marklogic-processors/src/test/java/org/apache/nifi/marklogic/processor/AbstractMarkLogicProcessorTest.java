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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.marklogic.controller.DefaultMarkLogicDatabaseClientService;
import org.apache.nifi.marklogic.controller.MarkLogicDatabaseClientService;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.*;
import org.junit.Assert;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class AbstractMarkLogicProcessorTest extends Assert {
    Processor processor;
    protected MockProcessContext processContext;
    protected MockProcessorInitializationContext initializationContext;
    protected SharedSessionState sharedSessionState;
    protected MockProcessSession processSession;
    protected TestRunner runner;
    protected MockProcessSessionFactory mockProcessSessionFactory;
    protected MarkLogicDatabaseClientService service;
    protected String databaseClientServiceIdentifier = "databaseClientService";

    protected ApplicationContext applicationContext;

    protected void initialize(Processor processor) {
        this.processor = processor;
        processContext = new MockProcessContext(processor);
        initializationContext = new MockProcessorInitializationContext(processor, processContext);
        sharedSessionState = new SharedSessionState(processor, new AtomicLong());
        processSession = new MockProcessSession(sharedSessionState, processor);
        mockProcessSessionFactory = new MockProcessSessionFactory(sharedSessionState, processor);
        runner = TestRunners.newTestRunner(processor);
        service = new DefaultMarkLogicDatabaseClientService();

        applicationContext = new AnnotationConfigApplicationContext(TestConfig.class);
        TestConfig testConfig = applicationContext.getBean(TestConfig.class);

        try {
            runner.addControllerService(databaseClientServiceIdentifier, service);
            
        } catch (InitializationException e) {
            throw new RuntimeException(e);
        }

        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.HOST, testConfig.getHost());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PORT, testConfig.getRestPort().toString());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.USERNAME, testConfig.getUsername());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PASSWORD, testConfig.getPassword());
    }

    /**
     * Configures the DatabaseClientService that the test runner can run a processor that depends on a
     * DatabaseClientService being configured.
     */
    protected void configureDatabaseClientService() {
        runner.enableControllerService(service);
        runner.assertValid(service);
    }
    protected MockFlowFile addTestFlowFile() {
        return addFlowFile("<test/>");
    }

    protected MockFlowFile addFlowFile(String... contents) {
        return addFlowFile(new HashMap(), contents);
    }

    protected MockFlowFile addFlowFile(Map<String, String> attributes, String... contents) {
        MockFlowFile flowFile = processSession.createFlowFile(String.join("\n", contents).getBytes(), attributes);
        sharedSessionState.getFlowFileQueue().offer(flowFile);
        return flowFile;
    }
}

class MockProcessSessionFactory implements ProcessSessionFactory {
    SharedSessionState sharedSessionState;
    Processor processor;
    MockProcessSessionFactory(SharedSessionState sharedSessionState, Processor processor) {
        this.sharedSessionState = sharedSessionState;
        this.processor = processor;
    }
    @Override
    public ProcessSession createSession() {
        return new MockProcessSession(sharedSessionState, processor);
    }
}