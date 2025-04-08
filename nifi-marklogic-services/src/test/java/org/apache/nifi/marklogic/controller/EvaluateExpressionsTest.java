package org.apache.nifi.marklogic.controller;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ext.SecurityContextType;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.util.MockConfigurationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EvaluateExpressionsTest {

    private DefaultMarkLogicDatabaseClientService service;
    private Map<PropertyDescriptor, String> properties;
    private Map<String, String> environmentVariables;
    private MockConfigurationContext context;

    @BeforeEach
    public void setup() {
        service = new DefaultMarkLogicDatabaseClientService();
        properties = new HashMap<>();
        environmentVariables = new HashMap<>();
        context = new MockConfigurationContext(properties, null, environmentVariables);
    }


    @Test
    public void evaluateHost() {
        verifyScope(DefaultMarkLogicDatabaseClientService.HOST);
        environmentVariables.put("myHost", "some-host");
        properties.put(DefaultMarkLogicDatabaseClientService.HOST, "${myHost}");
        assertEquals("some-host", service.buildDatabaseClientConfig(context).getHost());
    }

    @Test
    public void evaluatePort() {
        verifyScope(DefaultMarkLogicDatabaseClientService.PORT);
        environmentVariables.put("myPort", "8123");
        properties.put(DefaultMarkLogicDatabaseClientService.PORT, "${myPort}");
        assertEquals(8123, service.buildDatabaseClientConfig(context).getPort());
    }

    @Test
    public void evaluateSecurityContextType() {
        verifyScope(DefaultMarkLogicDatabaseClientService.SECURITY_CONTEXT_TYPE);
        environmentVariables.put("myType", SecurityContextType.KERBEROS.name());
        properties.put(DefaultMarkLogicDatabaseClientService.SECURITY_CONTEXT_TYPE, "${myType}");
        assertEquals(SecurityContextType.KERBEROS, service.buildDatabaseClientConfig(context).getSecurityContextType());
    }

    @Test
    public void evaluateUsername() {
        verifyScope(DefaultMarkLogicDatabaseClientService.USERNAME);
        environmentVariables.put("myUsername", "someone");
        properties.put(DefaultMarkLogicDatabaseClientService.USERNAME, "${myUsername}");
        assertEquals("someone", service.buildDatabaseClientConfig(context).getUsername());
    }

    @Test
    public void passwordDoesNotEvaluate() {
        assertEquals(ExpressionLanguageScope.NONE, DefaultMarkLogicDatabaseClientService.PASSWORD.getExpressionLanguageScope());
        environmentVariables.put("myPassword", "something");
        properties.put(DefaultMarkLogicDatabaseClientService.PASSWORD, "${myPassword}");
        assertEquals(
            "${myPassword}", service.buildDatabaseClientConfig(context).getPassword(),
            "Passwords should not be evaluated against the variable registry since variables only support plain text");
    }

    @Test
    public void evaluateDatabase() {
        verifyScope(DefaultMarkLogicDatabaseClientService.DATABASE);
        environmentVariables.put("myDatabase", "somedb");
        properties.put(DefaultMarkLogicDatabaseClientService.DATABASE, "${myDatabase}");
        assertEquals("somedb", service.buildDatabaseClientConfig(context).getDatabase());
    }

    @Test
    public void evaluateLoadBalancer() {
        verifyScope(DefaultMarkLogicDatabaseClientService.LOAD_BALANCER);
        environmentVariables.put("myType", "true");
        properties.put(DefaultMarkLogicDatabaseClientService.LOAD_BALANCER, "${myType}");
        assertEquals(DatabaseClient.ConnectionType.GATEWAY, service.buildDatabaseClientConfig(context).getConnectionType());
    }

    @Test
    public void evaluateExternalName() {
        verifyScope(DefaultMarkLogicDatabaseClientService.EXTERNAL_NAME);
        environmentVariables.put("myName", "somename");
        properties.put(DefaultMarkLogicDatabaseClientService.EXTERNAL_NAME, "${myName}");
        assertEquals("somename", service.buildDatabaseClientConfig(context).getExternalName());
    }

    @Test
    public void evaluateClientAuth() {
        verifyScope(DefaultMarkLogicDatabaseClientService.CLIENT_AUTH);
        environmentVariables.put("myValue", "WANT");
        properties.put(DefaultMarkLogicDatabaseClientService.CLIENT_AUTH, "${myValue}");
        assertEquals(ClientAuth.WANT, service.determineClientAuth(context));
    }

    private void verifyScope(PropertyDescriptor descriptor) {
        assertEquals(ExpressionLanguageScope.ENVIRONMENT, descriptor.getExpressionLanguageScope());
    }
}
