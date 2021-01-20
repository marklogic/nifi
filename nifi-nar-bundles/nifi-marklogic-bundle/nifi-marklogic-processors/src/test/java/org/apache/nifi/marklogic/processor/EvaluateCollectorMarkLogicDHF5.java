package org.apache.nifi.marklogic.processor;

import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.DocumentMetadataHandle.DocumentCollections;
import com.marklogic.junit5.spring.AbstractSpringMarkLogicTest;

import org.apache.nifi.marklogic.controller.DefaultMarkLogicDatabaseClientService;
import org.apache.nifi.marklogic.controller.MarkLogicDatabaseClientService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This just focuses on evaluating the collector script against MarkLogic, as that can't be done in
 * EvaluateCollectorMarkLogicTest.
 */
@ContextConfiguration(classes = {TestConfigDHF5.class})
public class EvaluateCollectorMarkLogicDHF5 extends AbstractSpringMarkLogicTest {

    @Autowired
    protected TestConfigDHF5 testConfig5;

    protected MarkLogicDatabaseClientService service;
    protected String databaseClientServiceIdentifier = "databaseClientService";

    protected void addDatabaseClientService(TestRunner runner) {
        service = new DefaultMarkLogicDatabaseClientService();
        try {
            runner.addControllerService(databaseClientServiceIdentifier, service);
        } catch (InitializationException e) {
            throw new RuntimeException(e);
        }
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.HOST, testConfig5.getHost());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PORT, testConfig5.getRestPort().toString());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.USERNAME, testConfig5.getUsername());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PASSWORD, testConfig5.getPassword());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.DATABASE,testConfig5.getDatabase());

        runner.enableControllerService(service);
    }

    protected TestRunner getNewTestRunner(Class processor) {
        TestRunner runner = TestRunners.newTestRunner(processor);
        addDatabaseClientService(runner);       
        assertTrue(runner.isControllerServiceEnabled(service));
        runner.assertValid(service);
        runner.setProperty(AbstractMarkLogicProcessor.DATABASE_CLIENT_SERVICE, databaseClientServiceIdentifier);
        return runner;
    }
    
    
	@BeforeEach
	public void setup() {
		JSONDocumentManager mgr = getDatabaseClient().newJSONDocumentManager();
        DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
        DocumentCollections collections = metadataHandle.getCollections();
        collections.add("EVALUATE-COLLECTOR-TEST");
        metadataHandle.setCollections(collections);
        
		for (int i = 0; i < 10; i++) {
			mgr.write("/sample/dhf5/" + i + ".json", metadataHandle,new StringHandle(format("{\"test\":\"%d\"}", i)));
		}
	}
	 
	@Override
		public void deleteDocumentsBeforeTestRuns() {
			// Removing test database clearing because we need DHF related documents in database. 
			System.out.println("Overriding : deleteDocumentsBeforeTestRuns");
		}
	
	@Test
	public void invokeCollector5() {
		TestRunner runner = getNewTestRunner(EvaluateCollectorMarkLogic.class);
		runner.setValidateExpressionUsage(false);
		runner.setProperty(EvaluateCollectorMarkLogic.ENTITY_NAME, "person");
		runner.setProperty(EvaluateCollectorMarkLogic.FLOW_NAME, "default");
		runner.setProperty(EvaluateCollectorMarkLogic.BATCH_SIZE, "4");
		runner.setProperty(EvaluateCollectorMarkLogic.DHF_VERSION, "5");
		runner.setProperty(EvaluateCollectorMarkLogic.STEP, "1");
		
		runner.run();

		List<MockFlowFile> list = runner.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals(3, list.size(), "For 10 docs and a batch size of 4, 3 FlowFiles should have been created");
	}

}
