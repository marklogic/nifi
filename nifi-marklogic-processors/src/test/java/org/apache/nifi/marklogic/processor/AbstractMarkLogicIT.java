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

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.DeleteListener;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import com.marklogic.junit5.spring.AbstractSpringMarkLogicTest;
import org.apache.nifi.marklogic.controller.DefaultMarkLogicDatabaseClientService;
import org.apache.nifi.marklogic.controller.MarkLogicDatabaseClientService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertTrue;

/**
 * Utilizes the JUnit5 test support provided by marklogic-junit - https://github.com/rjrudin/marklogic-junit .
 *
 * A DatabaseClient is constructed based on the properties loaded by the TestConfig class, which conveniently reads
 * from the Gradle properties files that are used to deploy the test application.
 *
 * The test database is cleared before every test is run so that no "residue" is left behind.
 */
@ContextConfiguration(classes = {TestConfig.class})
public class AbstractMarkLogicIT extends AbstractSpringMarkLogicTest {

    @Autowired
    protected TestConfig testConfig;

    protected MarkLogicDatabaseClientService service;
    protected String batchSize = "3";
    protected String threadCount = "3";
    protected String databaseClientServiceIdentifier = "databaseClientService";
    protected int numDocs = 30;

    protected int xmlMod = 5;
    protected int jsonMod = 3;
    protected int txtMod = 7;

    // mod  xmlMod == 0 docs are XML
    protected int expectedXmlCount = (int) (Math.ceil((numDocs - 1.0) / xmlMod));
    // mod jsonMod == 0 docs are JSON, but mod xmlMod == 0 docs are XML and take precedence in doc generation
    protected int expectedJsonCount = (int) (Math.ceil((numDocs - 1.0) / jsonMod) - Math.ceil((numDocs - 1.0) / (xmlMod * jsonMod)));

    protected DataMovementManager dataMovementManager;

    class IngestDoc {
        private Map<String, String> attributes;
        private String content;

        IngestDoc(String content) {
            this.attributes = new HashMap<>();
            this.content = content;
        }
        IngestDoc(String fileName, String content) {
            this(content);
            addAttribute("filename", fileName);
        }

        public String getFileName() {
            return attributes.getOrDefault("filename","uuid");
        }

        public String getContent() {
            return this.content;
        }

        public void addAttribute(String key, String value) {
            attributes.put(key, value);
        }

        public Map<String,String> getAttributes() {
            return this.attributes;
        }
    }
    protected List<IngestDoc> documents;

    protected void setup() {
        documents = new ArrayList<>(numDocs);
        dataMovementManager = getDatabaseClient().newDataMovementManager();
        for(int i = 0; i < numDocs; i++) {
            String fileName = "/PutMarkLogicTest/";
            String content = "";
            if(i % xmlMod == 0) {
                fileName += i + ".xml";
                content = "<root><sample>xmlcontent</sample><dateTime xmlns=\"namespace-test\">2000-01-01T00:00:00.000000</dateTime></root>";
            } else if ( i % jsonMod == 0) {
                fileName += i + ".json";
                content = "{\"sample\":\"jsoncontent\", \"dateTime\":\"2000-01-01T00:00:00.000000\"}";
            } else if (i % txtMod == 0) {
                fileName += i + ".txt";
                content = "A sample text document";
            } else {
                fileName += i + ".png";
                content = "7sndalsdamasa";
            }
            documents.add(new IngestDoc(fileName, content));
        }
    }

    protected void addDatabaseClientService(TestRunner runner) {
        service = new DefaultMarkLogicDatabaseClientService();
        try {
            runner.addControllerService(databaseClientServiceIdentifier, service);
        } catch (InitializationException e) {
            throw new RuntimeException(e);
        }
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.HOST, testConfig.getHost());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PORT, testConfig.getRestPort().toString());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.USERNAME, testConfig.getUsername());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PASSWORD, testConfig.getPassword());
        runner.enableControllerService(service);
    }

    protected TestRunner getNewTestRunner(Class processor) {
        TestRunner runner = TestRunners.newTestRunner(processor);
        addDatabaseClientService(runner);
        runner.setProperty(AbstractMarkLogicProcessor.BATCH_SIZE, batchSize);
        runner.setProperty(AbstractMarkLogicProcessor.THREAD_COUNT, threadCount);
        assertTrue(runner.isControllerServiceEnabled(service));
        runner.assertValid(service);
        runner.setProperty(AbstractMarkLogicProcessor.DATABASE_CLIENT_SERVICE, databaseClientServiceIdentifier);
        return runner;
    }

    protected int getNumDocumentsInCollection(String collection) {
        StructuredQueryDefinition collectionQuery = new StructuredQueryBuilder().collection(collection);
        AtomicInteger actualNumberOfDocs = new AtomicInteger(0);
        QueryBatcher queryBatcher = dataMovementManager.newQueryBatcher(collectionQuery)
                .onUrisReady(queryBatch -> actualNumberOfDocs.addAndGet(queryBatch.getItems().length));
        dataMovementManager.startJob(queryBatcher);
        queryBatcher.awaitCompletion();
        dataMovementManager.stopJob(queryBatcher);
        return actualNumberOfDocs.get();
    }

    protected void deleteDocumentsInCollection(String collection) {
        StructuredQueryDefinition collectionQuery = new StructuredQueryBuilder().collection(collection);
        QueryBatcher queryBatcher = dataMovementManager.newQueryBatcher(collectionQuery)
          .withConsistentSnapshot()
          .onUrisReady(new DeleteListener());
        dataMovementManager.startJob(queryBatcher);
        queryBatcher.awaitCompletion();
        dataMovementManager.stopJob(queryBatcher);
    }
}
