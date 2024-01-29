package org.apache.nifi.marklogic.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.SecurityContextType;
import com.marklogic.client.ext.modulesloader.ssl.SimpleX509TrustManager;
import com.marklogic.hub.DatabaseKind;
import com.marklogic.hub.flow.FlowInputs;
import com.marklogic.hub.impl.HubConfigImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RunFlowMarkLogicTest extends AbstractMarkLogicProcessorTest {

    private RunFlowMarkLogic processor;

    @BeforeEach
    public void setup() {
        processor = new RunFlowMarkLogic();
        initialize(processor);
    }

    @Test
    public void initializeHubConfig() {
        processContext.setProperty(RunFlowMarkLogic.FINAL_PORT, "9011");
        processContext.setProperty(RunFlowMarkLogic.JOB_PORT, "9013");

        DatabaseClientConfig config = new DatabaseClientConfig("somehost", 9010, "someuser", "someword");
        config.setSecurityContextType(SecurityContextType.BASIC);
        config.setSslContext(SimpleX509TrustManager.newSSLContext());
        config.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.STRICT);
        config.setExternalName("just-testing");

        HubConfigImpl hubConfig = processor.initializeHubConfig(processContext, config);
        assertEquals("somehost", hubConfig.getHost());

        assertEquals(Integer.valueOf(9010), hubConfig.getPort(DatabaseKind.STAGING));
        assertEquals(Integer.valueOf(9011), hubConfig.getPort(DatabaseKind.FINAL));
        assertEquals(Integer.valueOf(9013), hubConfig.getPort(DatabaseKind.JOB));

        assertEquals("someuser", hubConfig.getMlUsername());
        assertEquals("someword", hubConfig.getMlPassword());

        assertEquals("BASIC", hubConfig.getAuthMethod(DatabaseKind.STAGING));
        assertEquals("BASIC", hubConfig.getAuthMethod(DatabaseKind.JOB));
        assertEquals("BASIC", hubConfig.getAuthMethod(DatabaseKind.FINAL));

        assertNotNull(hubConfig.getSslHostnameVerifier(DatabaseKind.STAGING));
        assertNotNull(hubConfig.getSslHostnameVerifier(DatabaseKind.FINAL));
        assertNotNull(hubConfig.getSslHostnameVerifier(DatabaseKind.JOB));
        assertEquals(DatabaseClientFactory.SSLHostnameVerifier.STRICT, hubConfig.getSslHostnameVerifier(DatabaseKind.STAGING));
        assertEquals(DatabaseClientFactory.SSLHostnameVerifier.STRICT, hubConfig.getSslHostnameVerifier(DatabaseKind.FINAL));
        assertEquals(DatabaseClientFactory.SSLHostnameVerifier.STRICT, hubConfig.getSslHostnameVerifier(DatabaseKind.JOB));

        assertEquals("just-testing", hubConfig.getExternalName(DatabaseKind.STAGING));
        assertEquals("just-testing", hubConfig.getExternalName(DatabaseKind.FINAL));
        assertEquals("just-testing", hubConfig.getExternalName(DatabaseKind.JOB));
    }

    @Test
    public void buildFlowInputs() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode options = mapper.createObjectNode();
        options.put("sourceQuery", "cts.collectionQuery('this-is-typically-overridden-at-runtime')");

        processContext.setProperty(RunFlowMarkLogic.FLOW_NAME, "myFlow");
        processContext.setProperty(RunFlowMarkLogic.STEPS, "2,4,5");
        processContext.setProperty(RunFlowMarkLogic.JOB_ID, "myJobId");
        processContext.setProperty(RunFlowMarkLogic.OPTIONS_JSON, options.toString());

        FlowInputs inputs = processor.buildFlowInputs(processContext, addTestFlowFile());
        assertEquals("myFlow", inputs.getFlowName());
        assertEquals(3, inputs.getSteps().size());
        assertEquals("2", inputs.getSteps().get(0));
        assertEquals("4", inputs.getSteps().get(1));
        assertEquals("5", inputs.getSteps().get(2));
        assertEquals("myJobId", inputs.getJobId());

        Map<String, Object> map = inputs.getOptions();
        assertNotNull(map);
        assertEquals("cts.collectionQuery('this-is-typically-overridden-at-runtime')", map.get("sourceQuery"));
    }

    @Test
    public void buildMinimalFlowInputs() {
        processContext.setProperty(RunFlowMarkLogic.FLOW_NAME, "myFlow");
        FlowInputs inputs = processor.buildFlowInputs(processContext, addTestFlowFile());
        assertEquals("myFlow", inputs.getFlowName());
        assertNull(inputs.getSteps());
        assertNull(inputs.getJobId());
        assertNull(inputs.getOptions());
    }

    @Test
    public void configureSslWithNoTrustManager() {
        DatabaseClientConfig config = new DatabaseClientConfig("somehost", 9010, "someuser", "someword");
        final SSLContext sslContext = SimpleX509TrustManager.newSSLContext();
        config.setSslContext(sslContext);
        config.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.STRICT);
        config.setTrustManager(null);

        HubConfigImpl hubConfig = processor.initializeHubConfig(processContext, config);

        assertSame(sslContext, hubConfig.getSslContext(DatabaseKind.STAGING));
        assertSame(sslContext, hubConfig.getSslContext(DatabaseKind.FINAL));
        assertSame(sslContext, hubConfig.getSslContext(DatabaseKind.JOB));
        assertEquals(DatabaseClientFactory.SSLHostnameVerifier.STRICT, hubConfig.getSslHostnameVerifier(DatabaseKind.STAGING));
        assertEquals(DatabaseClientFactory.SSLHostnameVerifier.STRICT, hubConfig.getSslHostnameVerifier(DatabaseKind.FINAL));
        assertEquals(DatabaseClientFactory.SSLHostnameVerifier.STRICT, hubConfig.getSslHostnameVerifier(DatabaseKind.JOB));

        String message = "Since no trust manager was provided, a simple trust-everything one should be used";
        assertTrue(hubConfig.getTrustManager(DatabaseKind.STAGING) instanceof SimpleX509TrustManager, message);
        assertTrue(hubConfig.getTrustManager(DatabaseKind.FINAL) instanceof SimpleX509TrustManager, message);
        assertTrue(hubConfig.getTrustManager(DatabaseKind.JOB) instanceof SimpleX509TrustManager, message);
    }

    @Test
    public void configureSslWithTrustManager() {
        DatabaseClientConfig config = new DatabaseClientConfig("somehost", 9010, "someuser", "someword");
        final SSLContext sslContext = SimpleX509TrustManager.newSSLContext();
        final X509TrustManager trustManager = new SimpleX509TrustManager();
        config.setSslContext(sslContext);
        config.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.COMMON);
        config.setTrustManager(trustManager);

        HubConfigImpl hubConfig = processor.initializeHubConfig(processContext, config);

        assertSame(sslContext, hubConfig.getSslContext(DatabaseKind.STAGING));
        assertSame(sslContext, hubConfig.getSslContext(DatabaseKind.FINAL));
        assertSame(sslContext, hubConfig.getSslContext(DatabaseKind.JOB));
        assertEquals(DatabaseClientFactory.SSLHostnameVerifier.COMMON, hubConfig.getSslHostnameVerifier(DatabaseKind.STAGING));
        assertEquals(DatabaseClientFactory.SSLHostnameVerifier.COMMON, hubConfig.getSslHostnameVerifier(DatabaseKind.FINAL));
        assertEquals(DatabaseClientFactory.SSLHostnameVerifier.COMMON, hubConfig.getSslHostnameVerifier(DatabaseKind.JOB));

        String message = "Should use the trust manager in the DatabaseClientConfig, which will have been configured " +
            "by the user in the NiFi SSL service";
        assertSame(trustManager, hubConfig.getTrustManager(DatabaseKind.STAGING), message);
        assertSame(trustManager, hubConfig.getTrustManager(DatabaseKind.FINAL), message);
        assertSame(trustManager, hubConfig.getTrustManager(DatabaseKind.JOB), message);
    }
}
