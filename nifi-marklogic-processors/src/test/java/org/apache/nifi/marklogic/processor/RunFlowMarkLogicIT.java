package org.apache.nifi.marklogic.processor;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The test app does not yet have a DHF application available within it - it would really need to be converted into a
 * DHF application to make that feasible. For now, just verifying error handling in the processor.
 */
public class RunFlowMarkLogicIT extends AbstractMarkLogicIT {

    @Test
    void flowDoesntExist() {
        TestRunner runner = newReaderTestRunner(RunFlowMarkLogic.class);
        runner.setProperty(RunFlowMarkLogic.FLOW_NAME, "doesntExist");
        runner.run();

        assertEquals(0, runner.getFlowFilesForRelationship(RunFlowMarkLogic.FINISHED).size());

        List<MockFlowFile> files = runner.getFlowFilesForRelationship(RunFlowMarkLogic.FAILURE);
        assertEquals(1, files.size());
        String errorMessage = files.get(0).getAttribute("markLogicErrorMessage");
        assertTrue(errorMessage.startsWith("Unable to retrieve flow with name: doesntExist"),
            "Expecting the processor to fail because the flow can't be found, since the test doesn't even run " +
                "against a DHF application; error message: " + errorMessage);
    }
}
