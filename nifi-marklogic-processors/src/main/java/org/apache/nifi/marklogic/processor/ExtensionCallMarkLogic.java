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
package org.apache.nifi.marklogic.processor;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.extensions.ResourceManager;
import com.marklogic.client.extensions.ResourceServices.ServiceResultIterator;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.util.RequestParameters;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.util.*;
import java.util.regex.Pattern;

@Tags({"MarkLogic", "REST", "Extension", "Deprecated"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("DEPRECATED as of 1.16.3.1; allows MarkLogic REST extensions to be called. Deprecated due to the " +
        "output from the call to MarkLogic being appended to the incoming FlowFile which is unlikely to be desirable " +
        "behavior. CallRestExtensionMarkLogic should be used instead.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@DynamicProperty(name = "param: URL parameter, separator: separator to split values for a parameter.",
        value = "param: URL parameter, separator: separator to split values for a parameter.",
        description = "Depending on the property prefix, routes data to parameter, or splits parameter.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@Deprecated
public class ExtensionCallMarkLogic extends AbstractMarkLogicProcessor {

    public static final PropertyDescriptor EXTENSION_NAME = new PropertyDescriptor.Builder()
            .name("Extension Name")
            .displayName("Extension Name")
            .required(true)
            .description("Name of MarkLogic REST extension.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor REQUIRES_INPUT = new PropertyDescriptor.Builder()
            .name("Requires Input")
            .displayName("Requires Input")
            .required(true)
            .allowableValues("true", "false")
            .description("Whether an incoming FlowFile is required to run; should only be 'false' if the processor " +
                    "has no incoming connections")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor PAYLOAD_SOURCE = new PropertyDescriptor.Builder()
            .name("Payload Source")
            .displayName("Payload Source")
            .required(true)
            .description("Whether a payload body is passed and if so, from the FlowFile content or the Payload property.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(PayloadSources.allValues)
            .defaultValue(PayloadSources.NONE_STR)
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor PAYLOAD_FORMAT = new PropertyDescriptor.Builder()
            .name("Payload Format")
            .displayName("Payload Format")
            .required(true)
            .description("Format of request body payload.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(Format.JSON.name(), Format.XML.name(), Format.TEXT.name(), Format.BINARY.name(), Format.UNKNOWN.name())
            .defaultValue(Format.TEXT.name())
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor PAYLOAD = new PropertyDescriptor.Builder()
            .name("Payload")
            .displayName("Payload")
            .required(false)
            .description("Payload for request body if \"Payload Property\" is the selected Payload Type.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor METHOD_TYPE = new PropertyDescriptor.Builder()
            .name("Method Type")
            .displayName("Method Type")
            .required(false)
            .defaultValue(MethodTypes.POST_STR)
            .description("HTTP method to call the REST extension with.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(MethodTypes.allValues)
            .addValidator(Validator.VALID)
            .build();

    private volatile ExtensionResourceManager resourceManager;

    protected static final Relationship SUCCESS = new Relationship.Builder().name("success")
            .description("All items returned by the extension call to MarkLogic are appended to the content of the " +
                    "incoming FlowFile and sent to this relationship.")
            .build();

    protected static final Relationship FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that failed to produce a valid query.").build();

    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(DATABASE_CLIENT_SERVICE);
        list.add(EXTENSION_NAME);
        list.add(REQUIRES_INPUT);
        list.add(METHOD_TYPE);
        list.add(PAYLOAD_SOURCE);
        list.add(PAYLOAD_FORMAT);
        list.add(PAYLOAD);
        properties = Collections.unmodifiableList(list);
        Set<Relationship> set = new HashSet<>();
        set.add(SUCCESS);
        set.add(FAILURE);
        relationships = Collections.unmodifiableSet(set);
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.populatePropertiesByPrefix(context);
        DatabaseClient client = getDatabaseClient(context);
        String extensionName = context.getProperty(EXTENSION_NAME).evaluateAttributeExpressions(context.getAllProperties()).getValue();
        getLogger().info("Creating ResourceManager for REST extension: " + extensionName);
        resourceManager = new ExtensionResourceManager(client, extensionName);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();

        final String requiresInput = context.getProperty(REQUIRES_INPUT).getValue();
        FlowFile originalFlowFile = session.get();
        if ("true".equals(requiresInput) && originalFlowFile == null) {
            context.yield();
            return;
        } else if ("false".equals(requiresInput)) {
            originalFlowFile = session.create();
        }

        try {
            ServiceResultIterator results = callExtension(context, session, originalFlowFile);
            handleExtensionCallResults(results, session, originalFlowFile);
        } catch (Throwable t) {
            logErrorAndTransfer(t, originalFlowFile, session, FAILURE);
        }
    }

    /**
     * Extracted so it can be overridden by CallRestExtensionMarkLogic to provide what we think is the more
     * desirable behavior. This captures the behavior that has always existed in this processor.
     *
     * @param results
     * @param session
     * @param originalFlowFile
     */
    protected void handleExtensionCallResults(ServiceResultIterator results, ProcessSession session, FlowFile originalFlowFile) {
        if (results == null || !results.hasNext()) {
            transferAndCommit(session, originalFlowFile, SUCCESS);
            return;
        }
        try {
            while (results.hasNext()) {
                session.append(originalFlowFile, out -> out.write(results.next().getContent(new BytesHandle()).get()));
            }
        } finally {
            results.close();
            transferAndCommit(session, originalFlowFile, SUCCESS);
        }
    }

    private ServiceResultIterator callExtension(ProcessContext context, ProcessSession session, FlowFile originalFlowFile) {
        BytesHandle requestBody = buildRequestBody(context, session, originalFlowFile);
        RequestParameters requestParams = buildRequestParameters(context, originalFlowFile);
        String method = context.getProperty(METHOD_TYPE).getValue();
        return resourceManager.callService(method, requestBody, requestParams);
    }

    private BytesHandle buildRequestBody(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        BytesHandle requestBody = new BytesHandle();
        String payloadType = context.getProperty(PAYLOAD_SOURCE).getValue();
        switch (payloadType) {
            case PayloadSources.FLOWFILE_CONTENT_STR:
                final byte[] content = new byte[(int) flowFile.getSize()];
                session.read(flowFile, inputStream -> StreamUtils.fillBuffer(inputStream, content));
                requestBody.set(content);
                break;
            case PayloadSources.PAYLOAD_PROPERTY_STR:
                requestBody.set(context.getProperty(PAYLOAD).evaluateAttributeExpressions(flowFile).getValue().getBytes());
                break;
        }

        final String format = context.getProperty(PAYLOAD_FORMAT).getValue();
        if (format != null) {
            requestBody.withFormat(Format.valueOf(format));
        }

        return requestBody;
    }

    /**
     * Builds a Java Client RequestParameters object based on the dynamic "param:" properties found in the
     * given ProcessContext. These properties are evaluated against the given FlowFile attributes.
     *
     * @param context
     * @param flowFile
     * @return
     */
    private RequestParameters buildRequestParameters(ProcessContext context, FlowFile flowFile) {
        RequestParameters requestParameters = new RequestParameters();
        String paramPrefix = "param";
        List<PropertyDescriptor> parameterProperties = propertiesByPrefix.get(paramPrefix);
        if (parameterProperties != null) {
            for (final PropertyDescriptor propertyDesc : parameterProperties) {
                String paramName = propertyDesc.getName().substring(paramPrefix.length() + 1);
                String paramValue = context.getProperty(propertyDesc).evaluateAttributeExpressions(flowFile).getValue();
                PropertyValue separatorProperty = context.getProperty("separator:" + propertyDesc.getName());
                if (separatorProperty != null && separatorProperty.getValue() != null && !separatorProperty.getValue().isEmpty()) {
                    requestParameters.add(paramName, paramValue.split(Pattern.quote(separatorProperty.evaluateAttributeExpressions(flowFile).getValue())));
                } else {
                    requestParameters.add(paramName, paramValue);
                }
            }
        }
        return requestParameters;
    }

    private class ExtensionResourceManager extends ResourceManager {

        protected ExtensionResourceManager(DatabaseClient client, String resourceName) {
            super();
            client.init(resourceName, this);
        }

        protected ServiceResultIterator callService(String method, BytesHandle requestBody, RequestParameters parameters) {
            ServiceResultIterator serviceResultIterator;
            switch (method) {
                case MethodTypes.GET_STR:
                    serviceResultIterator = getServices().get(parameters);
                    break;
                case MethodTypes.POST_STR:
                    serviceResultIterator = getServices().post(parameters, requestBody);
                    break;
                case MethodTypes.PUT_STR:
                    // Java Client requires a non-null input for PUT calls
                    if (requestBody.get() == null) {
                        requestBody.set(new byte[]{});
                    }
                    serviceResultIterator = getServices().put(parameters, requestBody, null);
                    break;
                case MethodTypes.DELETE_STR:
                    serviceResultIterator = getServices().delete(parameters, null);
                    break;
                default:
                    serviceResultIterator = null;
            }
            return serviceResultIterator;
        }
    }

    public static class PayloadSources {

        public static final String NONE_STR = "None";
        public static final AllowableValue NONE = new AllowableValue(NONE_STR, NONE_STR,
                "No paylod is passed to the request body.");
        public static final String FLOWFILE_CONTENT_STR = "FlowFile Content";
        public static final AllowableValue FLOWFILE_CONTENT = new AllowableValue(FLOWFILE_CONTENT_STR, FLOWFILE_CONTENT_STR,
                "The FlowFile content is passed as a payload to the request body.");
        public static final String PAYLOAD_PROPERTY_STR = "Payload Property";
        public static final AllowableValue PAYLOAD_PROPERTY = new AllowableValue(PAYLOAD_PROPERTY_STR, PAYLOAD_PROPERTY_STR,
                "The Payload property is passed as a payload to the request body.");

        public static final AllowableValue[] allValues = new AllowableValue[]{NONE, FLOWFILE_CONTENT, PAYLOAD_PROPERTY};
    }

    public static class MethodTypes {

        public static final String POST_STR = "POST";
        public static final AllowableValue POST = new AllowableValue(POST_STR, POST_STR,
                "POST to REST extension");
        public static final String PUT_STR = "PUT";
        public static final AllowableValue PUT = new AllowableValue(PUT_STR, PUT_STR,
                "PUT to REST extension");
        public static final String GET_STR = "GET";
        public static final AllowableValue GET = new AllowableValue(GET_STR, GET_STR,
                "GET to REST extension");
        public static final String DELETE_STR = "DELETE";
        public static final AllowableValue DELETE = new AllowableValue(DELETE_STR, DELETE_STR,
                "DELETE to REST extension");

        public static final AllowableValue[] allValues = new AllowableValue[]{POST, PUT, GET, DELETE};
    }
}
