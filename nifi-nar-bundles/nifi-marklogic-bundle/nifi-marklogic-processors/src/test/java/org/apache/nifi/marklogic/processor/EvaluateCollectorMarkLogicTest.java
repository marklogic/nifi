package org.apache.nifi.marklogic.processor;

import com.marklogic.client.eval.EvalResult;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EvaluateCollectorMarkLogicTest extends AbstractMarkLogicProcessorTest {

	private EvaluateCollectorMarkLogic myProcessor;

	@Before
	public void setup() {
		myProcessor = new EvaluateCollectorMarkLogic();
		initialize(myProcessor);
	}

	@Test
	public void createFlowFilesWithDefaults1() {
		processContext.setProperty(EvaluateCollectorMarkLogic.BATCH_SIZE, "2");

		myProcessor.createFlowFilesForBatchesOfIdentifiers(processContext, processSession, addTestFlowFile(), buildMockResults(), null);

		List<MockFlowFile> newFlowFiles = processSession.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals("There should be 2 flow files, since our fake iterator had 3 ids in it, and the batch size is set to 2", 2, newFlowFiles.size());

		MockFlowFile firstBatch = newFlowFiles.get(0);
		firstBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id1,id2");
		firstBatch.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "1");
		firstBatch.assertAttributeNotExists(EvaluateCollectorMarkLogic.IDENTIFIERS_TOTAL_ATTRIBUTE);

		MockFlowFile secondBatch = newFlowFiles.get(1);
		secondBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id3");
		secondBatch.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "2");
		secondBatch.assertAttributeNotExists(EvaluateCollectorMarkLogic.IDENTIFIERS_TOTAL_ATTRIBUTE);
	}

	@Test
	public void createFlowFilesWithDefaults2() {
		processContext.setProperty(EvaluateCollectorMarkLogic.BATCH_SIZE, "2");

		myProcessor.createFlowFilesForBatchesOfIdentifiers(processContext, processSession, addTestFlowFile(), null, buildMockBufferedReader());

		List<MockFlowFile> newFlowFiles = processSession.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals("There should be 2 flow files, since our buffered reader had 3 ids in it, and the batch size is set to 2", 2, newFlowFiles.size());

		MockFlowFile firstBatch = newFlowFiles.get(0);
		firstBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id1,id2");
		firstBatch.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "1");
		firstBatch.assertAttributeNotExists(EvaluateCollectorMarkLogic.IDENTIFIERS_TOTAL_ATTRIBUTE);

		MockFlowFile secondBatch = newFlowFiles.get(1);
		secondBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id3");
		secondBatch.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "2");
		secondBatch.assertAttributeNotExists(EvaluateCollectorMarkLogic.IDENTIFIERS_TOTAL_ATTRIBUTE);
	}
	
	@Test
	public void createFlowFilesWithFragmentCountSet1() {
		processContext.setProperty(EvaluateCollectorMarkLogic.SET_FRAGMENT_COUNT, "true");
		processContext.setProperty(EvaluateCollectorMarkLogic.BATCH_SIZE, "2");

		myProcessor.createFlowFilesForBatchesOfIdentifiers(processContext, processSession, addTestFlowFile(), buildMockResults(),null);

		List<MockFlowFile> newFlowFiles = processSession.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals(2, newFlowFiles.size());

		MockFlowFile firstBatch = newFlowFiles.get(0);
		firstBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id1,id2");
		firstBatch.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "1");
		firstBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_TOTAL_ATTRIBUTE, "3");

		MockFlowFile secondBatch = newFlowFiles.get(1);
		secondBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id3");
		secondBatch.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "2");
		secondBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_TOTAL_ATTRIBUTE, "3");
	}
	
	@Test
	public void createFlowFilesWithFragmentCountSet2() {
		processContext.setProperty(EvaluateCollectorMarkLogic.SET_FRAGMENT_COUNT, "true");
		processContext.setProperty(EvaluateCollectorMarkLogic.BATCH_SIZE, "2");

		myProcessor.createFlowFilesForBatchesOfIdentifiers(processContext, processSession, addTestFlowFile(), null, buildMockBufferedReader());

		List<MockFlowFile> newFlowFiles = processSession.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals(2, newFlowFiles.size());

		MockFlowFile firstBatch = newFlowFiles.get(0);
		firstBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id1,id2");
		firstBatch.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "1");
		firstBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_TOTAL_ATTRIBUTE, "3");

		MockFlowFile secondBatch = newFlowFiles.get(1);
		secondBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id3");
		secondBatch.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "2");
		secondBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_TOTAL_ATTRIBUTE, "3");
	}
	
	@Test
	public void customDelimiter1() {
		processContext.setProperty(EvaluateCollectorMarkLogic.DELIMITER, ":");

		myProcessor.createFlowFilesForBatchesOfIdentifiers(processContext, processSession, addTestFlowFile(), buildMockResults(), null);

		List<MockFlowFile> newFlowFiles = processSession.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals("The batch size defaults to 100, and with 3 fake results, we should have one batch FlowFile", 1, newFlowFiles.size());

		MockFlowFile firstBatch = newFlowFiles.get(0);
		firstBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id1:id2:id3");
	}
	
	@Test
	public void customDelimiter2() {
		processContext.setProperty(EvaluateCollectorMarkLogic.DELIMITER, ":");

		myProcessor.createFlowFilesForBatchesOfIdentifiers(processContext, processSession, addTestFlowFile(), null, buildMockBufferedReader());

		List<MockFlowFile> newFlowFiles = processSession.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals("The batch size defaults to 100, and with 3 fake results, we should have one batch FlowFile", 1, newFlowFiles.size());

		MockFlowFile firstBatch = newFlowFiles.get(0);
		firstBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id1:id2:id3");
	}
	
	private MockEvalResultIterator buildMockResults() {
		List<EvalResult> results = new ArrayList<>();
		results.add(new StringEvalResult("id1"));
		results.add(new StringEvalResult("id2"));
		results.add(new StringEvalResult("id3"));
		return new MockEvalResultIterator(results);
	}
	
	private BufferedReader buildMockBufferedReader() {
        String s = "id1\nid2\nid3\n";
        StringReader strReader = new StringReader(s);
        BufferedReader mockBReader = new BufferedReader(strReader);
        return mockBReader;
	}
}
