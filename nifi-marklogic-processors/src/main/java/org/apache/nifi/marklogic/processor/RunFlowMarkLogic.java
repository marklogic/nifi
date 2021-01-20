package org.apache.nifi.marklogic.processor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
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
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.*;

@Tags({"MarkLogic", "Data Hub Framework"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Run a MarkLogic Data Hub flow. This is expected to be run on non-ingestion steps, where data " +
	"has already been ingested into MarkLogic. Ingestion steps depend on access to local files, which isn't a common " +
	"use case for NiFi in production.")
public class RunFlowMarkLogic extends AbstractMarkLogicProcessor {

	public static final PropertyDescriptor FINAL_PORT = new PropertyDescriptor.Builder()
		.name("Final Server Port")
		.displayName("Final Server Port")
		.required(true)
		.defaultValue("8011")
		.description("The port on which the Final REST server is hosted")
		.addValidator(StandardValidators.PORT_VALIDATOR)
		.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
		.build();

	public static final PropertyDescriptor JOB_PORT = new PropertyDescriptor.Builder()
		.name("Job Server Port")
		.displayName("Job Server Port")
		.required(true)
		.defaultValue("8013")
		.description("The port on which the Job REST server is hosted")
		.addValidator(StandardValidators.PORT_VALIDATOR)
		.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
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

		FlowFile flowFile = session.get();
		if (flowFile == null) {
			flowFile = session.create();
		}

		try {
			FlowInputs inputs = buildFlowInputs(context, flowFile);
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

			session.write(flowFile, out -> out.write(response.toJson().getBytes()));
			transferAndCommit(session, flowFile, FINISHED);
		} catch (Throwable t) {
			this.handleThrowable(t, session);
		}
	}

	protected HubConfigImpl initializeHubConfig(ProcessContext context, DatabaseClientConfig clientConfig) {
		HubConfigImpl hubConfig = HubConfigImpl.withDefaultProperties();

		hubConfig.setHost(clientConfig.getHost());
		hubConfig.setPort(DatabaseKind.STAGING, clientConfig.getPort());
		hubConfig.setMlUsername(clientConfig.getUsername());
		hubConfig.setMlPassword(clientConfig.getPassword());

		hubConfig.setPort(DatabaseKind.FINAL, context.getProperty(FINAL_PORT).evaluateAttributeExpressions().asInteger());
		hubConfig.setPort(DatabaseKind.JOB, context.getProperty(JOB_PORT).evaluateAttributeExpressions().asInteger());

		hubConfig.setAuthMethod(DatabaseKind.STAGING, clientConfig.getSecurityContextType().toString());
		hubConfig.setAuthMethod(DatabaseKind.FINAL, clientConfig.getSecurityContextType().toString());
		hubConfig.setAuthMethod(DatabaseKind.JOB, clientConfig.getSecurityContextType().toString());

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
