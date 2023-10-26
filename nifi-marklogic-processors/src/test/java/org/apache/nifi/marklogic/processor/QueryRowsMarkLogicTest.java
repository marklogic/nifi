package org.apache.nifi.marklogic.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryRowsMarkLogicTest extends AbstractMarkLogicProcessorTest {

    private QueryRowsMarkLogic myProcessor;
    private Map<String, String> attributes = new HashMap<>();

    @BeforeEach
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
        assertEquals(
            "application/json", myProcessor.determineMimeType(processContext, addFlowFile(attributes, "content")),
            "The MIMETYPE property value should be evaluated against the FlowFile attributes");
    }

    @Test
    public void evaluatePlan() {
        processContext.setProperty(QueryRowsMarkLogic.PLAN, "${thePlan}");
        attributes.put("thePlan", "anything");
        assertEquals(
            "anything", myProcessor.determineJsonPlan(processContext, addFlowFile(attributes, "content")),
            "The serialized plan should be evaluated against the FlowFile attributes");
    }
}
