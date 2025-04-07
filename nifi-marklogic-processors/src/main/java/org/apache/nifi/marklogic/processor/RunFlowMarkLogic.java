package org.apache.nifi.marklogic.processor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.modulesloader.ssl.SimpleX509TrustManager;
import com.marklogic.hub.DatabaseKind;
import com.marklogic.hub.flow.FlowInputs;
import com.marklogic.hub.flow.FlowRunner;
import com.marklogic.hub.flow.RunFlowResponse;
import com.marklogic.hub.flow.impl.FlowRunnerImpl;
import com.marklogic.hub.impl.HubConfigImpl;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.marklogic.controller.MarkLogicDatabaseClientService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Tags({"MarkLogic", "Data Hub Framework"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Run a MarkLogic Data Hub 6.1.x flow via the Data Hub 6.1.1 Java API. This is expected to be run on non-ingestion steps, where data " +
    "has already been ingested into MarkLogic. Ingestion steps depend on access to local files, which isn't a common " +
    "use case for NiFi in production. Note that a given version of the Data Hub Java API is not yet guaranteed to work " +
    "with any other version of Data Hub. It is thus recommended to only use this against a Data Hub 6.1.0 or 6.1.1 installation, " +
    "though it may work with other versions of Data Hub. Requires a MarkLogic user that is able to run the flow; " +
    "consult your Data Hub Framework documentation for guidelines on what Data Hub roles are required.")
public class RunFlowMarkLogic extends AbstractMarkLogicProcessor {

    public static final PropertyDescriptor FINAL_PORT = new PropertyDescriptor.Builder()
        .name("Final Server Port")
        .displayName("Final Server Port")
        .required(true)
        .defaultValue("8011")
        .description("The port on which the Final REST server is hosted")
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor JOB_PORT = new PropertyDescriptor.Builder()
        .name("Job Server Port")
        .displayName("Job Server Port")
        .required(true)
        .defaultValue("8013")
        .description("The port on which the Job REST server is hosted")
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor FLOW_NAME = new PropertyDescriptor.Builder()
        .name("Flow Name")
        .displayName("Flow Name")
        .description("Name of the Data Hub flow to run")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    public static final PropertyDescriptor STEPS = new PropertyDescriptor.Builder()
        .name("Steps")
        .displayName("Steps")
        .description("Comma-delimited string of step numbers to run")
        .addValidator(Validator.VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    public static final PropertyDescriptor JOB_ID = new PropertyDescriptor.Builder()
        .name("Job ID")
        .displayName("Job ID")
        .description("ID for the Data Hub job")
        .required(false)
        .addValidator(Validator.VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    public static final PropertyDescriptor OPTIONS_JSON = new PropertyDescriptor.Builder()
        .name("Options JSON")
        .displayName("Options JSON")
        .description("JSON object defining options for running the flow")
        .required(false)
        .addValidator(Validator.VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    protected static final Relationship FAILURE = new Relationship.Builder()
        .name("failure")
        // This is set to true for the 1.16.3.3 release so as to not break any existing instances of this processor.
        .autoTerminateDefault(true)
        .description("Captures any error that occurs in this processor.")
        .build();

    public static final Relationship FINISHED = new Relationship.Builder()
        .name("finished")
        .description("If the flow finishes, then regardless of its outcome, the JSON response will be sent to this relationship. " +
            "If an exception is thrown when running the flow, then a NiFi ProcessException will be thrown instead.")
        .build();

    private HubConfigImpl hubConfig;

    @Override
    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(DATABASE_CLIENT_SERVICE);
        list.add(FINAL_PORT);
        list.add(JOB_PORT);
        list.add(FLOW_NAME);
        list.add(STEPS);
        list.add(JOB_ID);
        list.add(OPTIONS_JSON);
        properties = Collections.unmodifiableList(list);

        Set<Relationship> set = new HashSet<>();
        set.add(FINISHED);
        set.add(FAILURE);
        relationships = Collections.unmodifiableSet(set);
    }

    /**
     * When the processor is scheduled, initialize a HubConfigImpl based on the inputs provided to the
     * MarkLogicDatabaseClientService. The DatabaseClient from this service cannot be reused, as DHF instantiates its
     * own set of DatabaseClients.
     *
     * @param context
     */
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        DatabaseClientConfig clientConfig = context.getProperty(DATABASE_CLIENT_SERVICE)
            .asControllerService(MarkLogicDatabaseClientService.class)
            .getDatabaseClientConfig();
        getLogger().info("Initializing HubConfig");
        this.hubConfig = initializeHubConfig(context, clientConfig);
        getLogger().info("Initialized HubConfig");
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();

        FlowFile incomingFlowFile = session.get();
        if (incomingFlowFile == null) {
            incomingFlowFile = session.create();
        }

        try {
            FlowInputs inputs = buildFlowInputs(context, incomingFlowFile);
            FlowRunner flowRunner = new FlowRunnerImpl(hubConfig);
            if (inputs.getSteps() != null) {
                getLogger().info(String.format("Running steps %s in flow %s", inputs.getSteps(), inputs.getFlowName()));
            } else {
                getLogger().info(String.format("Running flow %s", inputs.getFlowName()));
            }

            RunFlowResponse response = flowRunner.runFlow(inputs);
            flowRunner.awaitCompletion();
            if (inputs.getSteps() != null) {
                getLogger().info(String.format("Finished running steps %s in flow %s", inputs.getSteps(), inputs.getFlowName()));
            } else {
                getLogger().info(String.format("Finished running flow %s", inputs.getFlowName()));
            }

            session.write(incomingFlowFile, out -> out.write(response.toJson().getBytes()));
            transferAndCommit(session, incomingFlowFile, FINISHED);
        } catch (Throwable t) {
            logErrorAndTransfer(t, incomingFlowFile, session, FAILURE);
        }
    }

    protected HubConfigImpl initializeHubConfig(ProcessContext context, DatabaseClientConfig clientConfig) {
        Properties props = new Properties();
        props.setProperty("mlHost", clientConfig.getHost());
        props.setProperty("mlStagingPort", clientConfig.getPort() + "");
        props.setProperty("mlUsername", clientConfig.getUsername());
        props.setProperty("mlPassword", clientConfig.getPassword());
        props.setProperty("mlFinalPort", context.getProperty(FINAL_PORT).evaluateAttributeExpressions().getValue());
        props.setProperty("mlJobPort", context.getProperty(JOB_PORT).evaluateAttributeExpressions().getValue());

        props.setProperty("mlStagingAuth", clientConfig.getSecurityContextType().toString());
        props.setProperty("mlFinalAuth", clientConfig.getSecurityContextType().toString());
        props.setProperty("mlJobAuth", clientConfig.getSecurityContextType().toString());

        HubConfigImpl hubConfig = HubConfigImpl.withProperties(props);

        SSLContext sslContext = clientConfig.getSslContext();
        if (sslContext != null) {
            hubConfig.setSslContext(DatabaseKind.STAGING, sslContext);
            hubConfig.setSslContext(DatabaseKind.FINAL, sslContext);
            hubConfig.setSslContext(DatabaseKind.JOB, sslContext);
            DatabaseClientFactory.SSLHostnameVerifier verifier = clientConfig.getSslHostnameVerifier();
            if (verifier != null) {
                hubConfig.setSslHostnameVerifier(DatabaseKind.STAGING, verifier);
                hubConfig.setSslHostnameVerifier(DatabaseKind.FINAL, verifier);
                hubConfig.setSslHostnameVerifier(DatabaseKind.JOB, verifier);
            }

            X509TrustManager trustManager = clientConfig.getTrustManager();
            if (trustManager == null) {
                getLogger().info("No X509TrustManager found; using 'trust everything' X509TrustManager implementation");
                trustManager = new SimpleX509TrustManager();
            }
            hubConfig.setTrustManager(DatabaseKind.STAGING, trustManager);
            hubConfig.setTrustManager(DatabaseKind.FINAL, trustManager);
            hubConfig.setTrustManager(DatabaseKind.JOB, trustManager);
        }

        String externalName = clientConfig.getExternalName();
        if (externalName != null) {
            hubConfig.setExternalName(DatabaseKind.STAGING, externalName);
            hubConfig.setExternalName(DatabaseKind.FINAL, externalName);
            hubConfig.setExternalName(DatabaseKind.JOB, externalName);
        }

        return hubConfig;
    }

    protected FlowInputs buildFlowInputs(ProcessContext context, FlowFile flowFile) {
        final String flowName = context.getProperty(FLOW_NAME).evaluateAttributeExpressions(flowFile).getValue();
        FlowInputs inputs = new FlowInputs(flowName);

        if (context.getProperty(STEPS) != null) {
            String steps = context.getProperty(STEPS).evaluateAttributeExpressions(flowFile).getValue();
            if (!StringUtils.isEmpty(steps)) {
                inputs.setSteps(Arrays.asList(steps.split(",")));
            }
        }

        if (context.getProperty(JOB_ID) != null) {
            inputs.setJobId(context.getProperty(JOB_ID).evaluateAttributeExpressions(flowFile).getValue());
        }

        if (context.getProperty(OPTIONS_JSON) != null) {
            String options = context.getProperty(OPTIONS_JSON).evaluateAttributeExpressions(flowFile).getValue();
            if (!StringUtils.isEmpty(options)) {
                Map<String, Object> map;
                try {
                    map = new ObjectMapper().readValue(options, new TypeReference<Map<String, Object>>() {
                    });
                } catch (IOException e) {
                    throw new ProcessException("Unable to parse JSON options: " + e.getMessage(), e);
                }
                inputs.setOptions(map);
            }
        }

        return inputs;
    }
}
