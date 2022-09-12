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
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.RawStructuredQueryDefinition;
import com.marklogic.client.query.StructuredQueryDefinition;
import org.apache.nifi.marklogic.processor.util.QueryTypes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.junit.Before;
import org.junit.Test;

public class QueryMarkLogicTest extends AbstractMarkLogicProcessorTest {

    private TestQueryMarkLogic processor;

    @Before
    public void setup() throws InitializationException {
        processor = new TestQueryMarkLogic();
        initialize(processor);
        processContext.setProperty(TestQueryMarkLogic.THREAD_COUNT.getName(), "5");
        processContext.setProperty(TestQueryMarkLogic.BATCH_SIZE, "15");
        processContext.setProperty(TestQueryMarkLogic.QUERY_TYPE, QueryTypes.COLLECTION.getValue());
        processContext.setProperty(TestQueryMarkLogic.QUERY, "test");
        processContext.setProperty(TestQueryMarkLogic.DATABASE_CLIENT_SERVICE, databaseClientServiceIdentifier);
    }

    private TestQueryBatcher runProcessorAndReturnQueryDefinition() {
        processor.initialize(initializationContext);
        processor.onTrigger(processContext, mockProcessSessionFactory);
        return (TestQueryBatcher) processor.getQueryBatcherForTesting();
    }

    @Test
    public void testCollectionsQueryMarkLogic() {
        runner.enableControllerService(service);
        runner.assertValid(service);

        processContext.setProperty(TestQueryMarkLogic.QUERY_TYPE, QueryTypes.COLLECTION.getValue());

        TestQueryBatcher queryBatcher = runProcessorAndReturnQueryDefinition();
        assertEquals(15, queryBatcher.getBatchSize());
        assertEquals(5, queryBatcher.getThreadCount());
        assertTrue(queryBatcher.getQueryDefinition() instanceof StructuredQueryDefinition);
    }

    @Test
    public void testCombinedJsonQueryMarkLogic() {
        processContext.setProperty(TestQueryMarkLogic.QUERY_TYPE, QueryTypes.COMBINED_JSON.getValue());

        TestQueryBatcher queryBatcher = runProcessorAndReturnQueryDefinition();
        assertTrue(queryBatcher.getQueryDefinition() instanceof RawCombinedQueryDefinition);
        StringHandle handle = (StringHandle) ((RawCombinedQueryDefinition) queryBatcher.getQueryDefinition()).getHandle();
        assertEquals(handle.getFormat(), Format.JSON);
    }

    @Test
    public void testCombinedXmlQueryMarkLogic() {
        processContext.setProperty(TestQueryMarkLogic.QUERY_TYPE, QueryTypes.COMBINED_XML.getValue());

        TestQueryBatcher queryBatcher = runProcessorAndReturnQueryDefinition();
        assertTrue(queryBatcher.getQueryDefinition() instanceof RawCombinedQueryDefinition);
        StringHandle handle = (StringHandle) ((RawCombinedQueryDefinition) queryBatcher.getQueryDefinition()).getHandle();
        assertEquals(handle.getFormat(), Format.XML);
    }

    @Test
    public void testStructuredJsonQueryMarkLogic() {
        processContext.setProperty(TestQueryMarkLogic.CONSISTENT_SNAPSHOT, "false");
        processContext.setProperty(TestQueryMarkLogic.QUERY_TYPE, QueryTypes.STRUCTURED_JSON.getValue());

        TestQueryBatcher queryBatcher = runProcessorAndReturnQueryDefinition();
        assertTrue(queryBatcher.getQueryDefinition() instanceof RawStructuredQueryDefinition);
        StringHandle handle = (StringHandle) ((RawStructuredQueryDefinition) queryBatcher.getQueryDefinition()).getHandle();
        assertEquals(handle.getFormat(), Format.JSON);
    }

    @Test
    public void testStructuredXmlQueryMarkLogic() {
        processContext.setProperty(TestQueryMarkLogic.QUERY_TYPE, QueryTypes.STRUCTURED_XML.getValue());

        TestQueryBatcher queryBatcher = runProcessorAndReturnQueryDefinition();
        assertTrue(queryBatcher.getQueryDefinition() instanceof RawStructuredQueryDefinition);
        StringHandle handle = (StringHandle) ((RawStructuredQueryDefinition) queryBatcher.getQueryDefinition()).getHandle();
        assertEquals(handle.getFormat(), Format.XML);
    }

    @Test
    public void testRetrieveFullDocumentsFalse() {
        processContext.setProperty(TestQueryMarkLogic.RETURN_TYPE, QueryMarkLogic.ReturnTypes.URIS_ONLY_STR);
        processContext.setProperty(TestQueryMarkLogic.QUERY_TYPE, QueryTypes.STRUCTURED_XML.getValue());

        TestQueryBatcher queryBatcher = runProcessorAndReturnQueryDefinition();
        assertTrue(queryBatcher.getQueryDefinition() instanceof RawStructuredQueryDefinition);
        StringHandle handle = (StringHandle) ((RawStructuredQueryDefinition) queryBatcher.getQueryDefinition()).getHandle();
        assertEquals(handle.getFormat(), Format.XML);
    }

    class TestQueryMarkLogic extends QueryMarkLogic {
        @Override
        public DatabaseClient getDatabaseClient(ProcessContext context) {
            return new TestMLDatabaseClient();
        }
    }

}
