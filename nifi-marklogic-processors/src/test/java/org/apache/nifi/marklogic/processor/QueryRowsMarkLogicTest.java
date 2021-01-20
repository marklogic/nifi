package org.apache.nifi.marklogic.processor;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class QueryRowsMarkLogicTest extends AbstractMarkLogicProcessorTest {

	private QueryRowsMarkLogic myProcessor;
	private Map<String, String> attributes = new HashMap<>();

	@Before
	public void setup() {
		myProcessor = new QueryRowsMarkLogic();
		initialize(myProcessor);
	}

	@Test
	public void defaultMimeType() {
		assertEquals("text/csv", myProcessor.determineMimeType(processContext, addTestFlowFile()));
	}

	@Test
	public void evaluateMimeType() {
		processContext.setProperty(QueryRowsMarkLogic.MIMETYPE, "${mimeType}");
		attributes.put("mimeType", "application/json");
		assertEquals("The MIMETYPE property value should be evaluated against the FlowFile attributes",
			"application/json", myProcessor.determineMimeType(processContext, addFlowFile(attributes, "content")));
	}

	@Test
	public void evaluatePlan() {
		processContext.setProperty(QueryRowsMarkLogic.PLAN, "${thePlan}");
		attributes.put("thePlan", "anything");
		assertEquals("The serialized plan should be evaluated against the FlowFile attributes",
			"anything", myProcessor.determineJsonPlan(processContext, addFlowFile(attributes, "content")));
	}
}
