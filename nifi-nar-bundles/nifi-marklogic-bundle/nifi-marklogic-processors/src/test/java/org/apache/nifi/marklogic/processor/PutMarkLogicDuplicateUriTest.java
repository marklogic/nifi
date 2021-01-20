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


import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PutMarkLogicDuplicateUriTest extends AbstractMarkLogicProcessorTest {

	private TestDuplicatePutMarkLogic processor;
	
	@BeforeEach
	public void setup() throws InitializationException {
		processor = new TestDuplicatePutMarkLogic();
		initialize(processor);

	}
	@Test
	public void testDuplicateUriIgnoreHandling() {
		processContext.setProperty(PutMarkLogic.DUPLICATE_URI_HANDLING, PutMarkLogic.IGNORE);
		processContext.setProperty(PutMarkLogic.FORMAT, Format.JSON.name());
		processContext.setProperty(PutMarkLogic.URI_ATTRIBUTE_NAME, "id");
		processor.initialize(initializationContext);
		processor.reset();

		// Just use same id
		Map<String, String> attributes = new HashMap<>();
		attributes.put("id", "123456.json");

		MockFlowFile flowFile = addFlowFile(attributes, "{\"hello\":\"nifi rocks\"}");
		processor.onTrigger(processContext, mockProcessSessionFactory);
		MockFlowFile flowFile2 = addFlowFile(attributes, "{\"hello\":\"nifi rocks\"}");
		processor.onTrigger(processContext, mockProcessSessionFactory);
		MockFlowFile flowFile3 = addFlowFile(attributes, "{\"hello\":\"nifi rocks\"}");

		processor.onTrigger(processContext, mockProcessSessionFactory);

		assertEquals("Should only be 3 uri in uriFlowFileMap", 3, TestDuplicatePutMarkLogic.uriFlowFileMap.size());
		assertEquals("Should only be 0 uri in duplicateFlowFileMap", 0,	TestDuplicatePutMarkLogic.duplicateFlowFileMap.size());
		assertEquals(3, processor.writeEventsCount);
		processor.onScheduled(processContext);
	}

	@Test
	public void testDuplicateUriFailureHandling() {
		processContext.setProperty(PutMarkLogic.DUPLICATE_URI_HANDLING, PutMarkLogic.FAIL_URI);
		processContext.setProperty(PutMarkLogic.FORMAT, Format.JSON.name());
		processContext.setProperty(PutMarkLogic.URI_ATTRIBUTE_NAME, "id");
		processor.initialize(initializationContext);
		processor.reset();

		// Just use same id
		Map<String, String> attributes = new HashMap<>();
		attributes.put("id", "123456.json");

		MockFlowFile flowFile = addFlowFile(attributes, "{\"hello\":\"nifi rocks\"}");
		processor.onTrigger(processContext, mockProcessSessionFactory);
		MockFlowFile flowFile2 = addFlowFile(attributes, "{\"hello\":\"nifi rocks\"}");
		processor.onTrigger(processContext, mockProcessSessionFactory);
		MockFlowFile flowFile3 = addFlowFile(attributes, "{\"hello\":\"nifi rocks\"}");

		processor.onTrigger(processContext, mockProcessSessionFactory);

		assertEquals("Should only be 1 uri in uriFlowFileMap", 1, TestDuplicatePutMarkLogic.uriFlowFileMap.size());
		assertEquals("Should only be 1 uri in duplicateFlowFileMap", 1,
				TestDuplicatePutMarkLogic.duplicateFlowFileMap.size());
		assertEquals("The first flowFile UUID should be the currentFlowFileUUID ",
				flowFile.getAttribute(CoreAttributes.UUID.key()), processor.lastUUID);
		assertEquals("Should be only 1 writeEvent",1, processor.writeEventsCount);
		processor.onScheduled(processContext);
	}
	@Test
	public void testDuplicateUriUseCloseBatchHandling() {
		processContext.setProperty(PutMarkLogic.DUPLICATE_URI_HANDLING, PutMarkLogic.CLOSE_BATCH);
		processContext.setProperty(PutMarkLogic.FORMAT, Format.JSON.name());
		processContext.setProperty(PutMarkLogic.URI_ATTRIBUTE_NAME, "id");
		processor.initialize(initializationContext);
		processor.reset();
		// Just use same id
		Map<String, String> attributes = new HashMap<>();
		attributes.put("id", "123456.json");
		attributes.put("index", "1");
		MockFlowFile flowFile1 = addFlowFile(attributes, "{\"hello\":\"nifi rocks\"}");
		processor.onTrigger(processContext, mockProcessSessionFactory);
		attributes.put("index", "2");
		MockFlowFile flowFile2 = addFlowFile(attributes, "{\"hello\":\"nifi rocks\"}");
		processor.onTrigger(processContext, mockProcessSessionFactory);
		attributes.put("index", "3");
		MockFlowFile flowFile3 = addFlowFile(attributes, "{\"hello\":\"nifi rocks\"}");
		// The last superseded flow file is always the flowFile2
		String lastFlowUUID = flowFile2.getAttribute(CoreAttributes.UUID.key());
		processor.onTrigger(processContext, mockProcessSessionFactory);
		//assertEquals("CloseBatchWriter should be 2",2,processor.closeWriterBatcherCount);
		//processor.onScheduled(processContext);
	}
}

/**
 * This subclass allows us to intercept the calls to WriteBatcher so that no
 * calls are made to MarkLogic.
 */

class TestDuplicatePutMarkLogic extends PutMarkLogic {
	public boolean flushAsyncCalled = false;
	public WriteEvent writeEvent;
	public int writeEventsCount = 0;
	public int failedCount = 0;
	public int closeWriterBatcherCount = 0;
	public String lastSupersededIndex = "";
	public String lastSupersededUUID = "";
	public String lastUUID = "";
	public HashMap<String, Integer> relationsMap = new HashMap<String, Integer>();
	//Clear the maps and variables so we can clean between tests.
	void reset() {
		writeEventsCount = 0;
		failedCount = 0;
		relationsMap.clear();
		TestDuplicatePutMarkLogic.uriFlowFileMap.clear();
		TestDuplicatePutMarkLogic.duplicateFlowFileMap.clear();
	}

	@Override
	protected void flushWriteBatcherAsync(WriteBatcher writeBatcher) {
		flushAsyncCalled = true;
		writeEventsCount = 0;
	}
	@Override
	protected void routeDocumentToRelationship(WriteEvent writeEvent, Relationship relationship) {
		String relName = relationship.getName();
		FlowFileInfo fileInfo = getFlowFileInfoForWriteEvent(writeEvent);
		int relCtr = relationsMap.get(relName) != null ? relationsMap.get(relName) : 0;
		relationsMap.put(relName, relCtr++);
		if (relName.equals("supersedes")) {
			lastSupersededIndex = fileInfo.flowFile.getAttribute("index");
			lastSupersededUUID = fileInfo.flowFile.getAttribute(CoreAttributes.UUID.key());
		}
		//Just route to super to ensure it works properly
		super.routeDocumentToRelationship(writeEvent, relationship);
	}

	@Override
	protected void addWriteEvent(WriteBatcher writeBatcher, WriteEvent writeEvent) {
		DocumentMetadataHandle metadata = (DocumentMetadataHandle) writeEvent.getMetadata();
		lastUUID = metadata.getMetadataValues().get("flowFileUUID");
		this.writeEvent = writeEvent;
		writeEventsCount++;
		getLogger().info("Writing URI:" + writeEvent.getTargetUri() + ",flowfileaUUID:"+lastUUID);
	}
	@Override
	public void closeWriteBatcher() {
		closeWriterBatcherCount++;
	}
}
