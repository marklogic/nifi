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
package org.apache.nifi.marklogic.controller;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.client.ext.SecurityContextType;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.ProviderException;
import java.util.*;

@Tags({"MarkLogic"})
@CapabilityDescription("Provides a MarkLogic DatabaseClient instance for use by other processors")
public class DefaultMarkLogicDatabaseClientService extends AbstractControllerService implements MarkLogicDatabaseClientService {

    private static List<PropertyDescriptor> properties;

    private DatabaseClient databaseClient;
    private DatabaseClientConfig databaseClientConfig;

    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
        .name("Host")
        .displayName("Host")
        .required(true)
        .defaultValue("localhost")
        .description("The hostname of the MarkLogic server or load balancer to connect to.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
        .name("Port")
        .displayName("Port")
        .required(true)
        .defaultValue("8000")
        .description("The port of the MarkLogic app server that supports the MarkLogic Client REST API.")
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor LOAD_BALANCER = new PropertyDescriptor.Builder()
        .name("Load Balancer")
        .displayName("Load Balancer")
        .description("Set to 'true' if the host defined by the 'Host' property is a load balancer; otherwise, set to 'false' or leave blank.")
        .allowableValues("true", "false")
        .defaultValue("false")
        .addValidator(Validator.VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor BASE_PATH = new PropertyDescriptor.Builder()
        .name("Base Path")
        .displayName("Base Path")
        .required(false)
        .description("Base path to prepend for all calls to MarkLogic REST API; typically set when using a load balancer in front of MarkLogic.")
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor SECURITY_CONTEXT_TYPE = new PropertyDescriptor.Builder()
        .name("Security Context Type")
        .displayName("Security Context Type")
        .required(true)
        .description("The type of the Security Context that needs to be used for authentication; " +
            "must be one of 'BASIC', 'DIGEST', 'CLOUD', 'CERTIFICATE', or 'KERBEROS'.")
        .allowableValues(
            SecurityContextType.BASIC.name(),
            SecurityContextType.DIGEST.name(),
            SecurityContextType.CLOUD.name(),
            SecurityContextType.CERTIFICATE.name(),
            SecurityContextType.KERBEROS.name()
        )
        .defaultValue(SecurityContextType.DIGEST.name())
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("Username")
        .displayName("Username")
        .description("The MarkLogic user with sufficient privileges for using the MarkLogic Client REST API; required for Basic and Digest authentication.")
        .addValidator(Validator.VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
        .name("Password")
        .displayName("Password")
        .description("The password for the Marklogic user identified by 'Username'; required for Basic and Digest authentication.")
        .sensitive(true)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor CLOUD_API_KEY = new PropertyDescriptor.Builder()
        .name("Cloud API Key")
        .displayName("Cloud API Key")
        .description("The API key for authenticating with a Progress Data Cloud instance; typically requires setting 'Load Balancer' to 'true' as well.")
        .sensitive(true)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
        .name("Database")
        .displayName("Database")
        .description("The database to access, if not the one associated with the MarkLogic app server identified by 'Port'.")
        .addValidator(Validator.VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor EXTERNAL_NAME = new PropertyDescriptor.Builder()
        .name("External name")
        .displayName("External name")
        .description("External name of the Kerberos Client; required for Kerberos authentication.")
        .addValidator(Validator.VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("SSL Context Service")
        .displayName("SSL Context Service")
        .description("The SSL Context Service used to provide KeyStore and TrustManager information for secure connections.")
        .required(false)
        .identifiesControllerService(SSLContextService.class)
        .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
        .name("Client Authentication")
        .displayName("Client Authentication")
        .description("Client authentication policy when connecting via a secure connection; only used when an 'SSL Context Service' "
            + "has been defined. Must be one of 'Required', 'Want', or 'None', or left blank.")
        .required(false)
        .allowableValues(ClientAuth.values())
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    static {
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(HOST);
        list.add(PORT);
        list.add(LOAD_BALANCER);
        list.add(BASE_PATH);
        list.add(SECURITY_CONTEXT_TYPE);
        list.add(USERNAME);
        list.add(PASSWORD);
        list.add(CLOUD_API_KEY);
        list.add(DATABASE);
        list.add(EXTERNAL_NAME);
        list.add(SSL_CONTEXT_SERVICE);
        list.add(CLIENT_AUTH);
        properties = Collections.unmodifiableList(list);
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        getLogger().info("Creating DatabaseClient");
        databaseClientConfig = buildDatabaseClientConfig(context);
        databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);
    }

    @OnDisabled
    public void shutdown() {
        if (databaseClient != null) {
            databaseClient.release();
            databaseClient = null;
        }
    }

    protected DatabaseClientConfig buildDatabaseClientConfig(ConfigurationContext context) {
        DatabaseClientConfig config = new DatabaseClientConfig();
        config.setHost(context.getProperty(HOST).evaluateAttributeExpressions().getValue());
        Objects.requireNonNull(context.getProperty(PORT), "PORT property should not be null");
        config.setPort(context.getProperty(PORT).evaluateAttributeExpressions().asInteger());
        config.setBasePath(context.getProperty(BASE_PATH).evaluateAttributeExpressions().getValue());
        config.setSecurityContextType(SecurityContextType.valueOf(
            context.getProperty(SECURITY_CONTEXT_TYPE).evaluateAttributeExpressions().getValue())
        );
        config.setUsername(context.getProperty(USERNAME).evaluateAttributeExpressions().getValue());
        config.setPassword(context.getProperty(PASSWORD).getValue());
        config.setCloudApiKey(context.getProperty(CLOUD_API_KEY).getValue());
        config.setDatabase(context.getProperty(DATABASE).evaluateAttributeExpressions().getValue());

        if (context.getProperty(LOAD_BALANCER) != null &&
            context.getProperty(LOAD_BALANCER).evaluateAttributeExpressions().asBoolean() != null &&
            context.getProperty(LOAD_BALANCER).evaluateAttributeExpressions().asBoolean()) {
            config.setConnectionType(DatabaseClient.ConnectionType.GATEWAY);
        }

        if (context.getProperty(EXTERNAL_NAME) != null && !"".equals(context.getProperty(EXTERNAL_NAME).evaluateAttributeExpressions().getValue())) {
            config.setExternalName(context.getProperty(EXTERNAL_NAME).evaluateAttributeExpressions().getValue());
        }

        final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslService != null) {
            final ClientAuth clientAuth = determineClientAuth(context);
            getLogger().info("Configuring SSL connection; client authentication: " + clientAuth);
            try {
                if (sslService.isTrustStoreConfigured()) {
                    getLogger().info("Configuring TrustManager based on trust store found in SSLService");
                    final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    final KeyStore trustStore = KeyStore.getInstance(sslService.getTrustStoreType());
                    try (final InputStream trustStoreStream = new FileInputStream(sslService.getTrustStoreFile())) {
                        trustStore.load(trustStoreStream, sslService.getTrustStorePassword().toCharArray());
                    }
                    trustManagerFactory.init(trustStore);
                    config.setTrustManager((X509TrustManager) trustManagerFactory.getTrustManagers()[0]);
                }
                final SSLContext sslContext = sslService.createContext();
                config.setSslContext(sslContext);
                config.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
            } catch (Exception e) {
                getLogger().error("Failed to create SSLContext due to {}", new Object[]{e});
                throw new ProcessException(e);
            }
        }

        return config;
    }

    protected ClientAuth determineClientAuth(ConfigurationContext context) {
        try {
            return context.getProperty(CLIENT_AUTH).evaluateAttributeExpressions().getValue() == null ? ClientAuth.REQUIRED :
                ClientAuth.valueOf(context.getProperty(CLIENT_AUTH).evaluateAttributeExpressions().getValue());
        } catch (IllegalArgumentException exception) {
            throw new ProviderException("Client Authentication should be one of the following values : "
                + Arrays.toString(ClientAuth.values()));
        }
    }

    @Override
    public DatabaseClient getDatabaseClient() {
        return databaseClient;
    }

    @Override
    public DatabaseClientConfig getDatabaseClientConfig() {
        return databaseClientConfig;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

}
