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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.extensions.ResourceManager;
import com.marklogic.client.extensions.ResourceServices.ServiceResult;
import com.marklogic.client.extensions.ResourceServices.ServiceResultIterator;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.util.RequestParameters;

@Tags({"MarkLogic", "REST", "Extension"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("Allows MarkLogic REST extensions to be called")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@DynamicProperty(name = "param: URL parameter, separator: separator to split values for a parameter.",
        value = "param: URL parameter, separator: separator to split values for a parameter.",
        description = "Depending on the property prefix, routes data to parameter, or splits parameter.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@TriggerWhenEmpty
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
            .description("Whether a FlowFile is required to run.")
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

    public volatile ExtensionResourceManager resourceManager;
    public boolean requiresInput = true;

    protected static final Relationship SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are created from documents read from MarkLogic are routed to"
                    + " this success relationship.")
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
        resourceManager = new ExtensionResourceManager(client, extensionName);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        onTrigger(context, session);
    }

    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            FlowFile flowFile = session.get();
            if (requiresInput && flowFile == null) {
                context.yield();
                return;
            } else if (!requiresInput) {
                flowFile = session.create();
            }
            RequestParameters requestParameters = new RequestParameters();
            String paramPrefix = "param";
            List<PropertyDescriptor> parameterProperties = propertiesByPrefix.get(paramPrefix);
            if (parameterProperties != null) {
                for (final PropertyDescriptor propertyDesc : parameterProperties) {
                    String paramName = propertyDesc.getName().substring(paramPrefix.length() + 1);
                    String paramValue = context.getProperty(propertyDesc).evaluateAttributeExpressions(flowFile).getValue();
                    PropertyValue separatorProperty = context.getProperty("separator:" + propertyDesc.getName());
                    if (separatorProperty != null && separatorProperty.getValue() != null && !separatorProperty.getValue().isEmpty()) {
                        requestParameters.add(
                                paramName,
                                paramValue.split(Pattern.quote(separatorProperty.evaluateAttributeExpressions(flowFile).getValue()))
                        );
                    } else {
                        requestParameters.add(paramName, paramValue);
                    }
                }
            }
            String method = context.getProperty(METHOD_TYPE).getValue();
            BytesHandle bytesHandle = new BytesHandle();
            String payloadType = context.getProperty(PAYLOAD_SOURCE).getValue();
            switch (payloadType) {
                case PayloadSources.FLOWFILE_CONTENT_STR:
                    final byte[] content = new byte[(int) flowFile.getSize()];
                    session.read(flowFile, inputStream -> StreamUtils.fillBuffer(inputStream, content));
                    bytesHandle.set(content);
                    break;
                case PayloadSources.PAYLOAD_PROPERTY_STR:
                    bytesHandle.set(context.getProperty(PAYLOAD).evaluateAttributeExpressions(flowFile).getValue().getBytes());
                    break;
                default:
                    bytesHandle.set("\n".getBytes());
            }
            if (bytesHandle.get().length == 0) {
                bytesHandle.set("\n".getBytes());
            }
            final String format = context.getProperty(PAYLOAD_FORMAT).getValue();
            if (format != null) {
                bytesHandle.withFormat(Format.valueOf(format));
            }

            ServiceResultIterator resultIterator = resourceManager.callService(method, bytesHandle, requestParameters);
            if (resultIterator == null || !resultIterator.hasNext()) {
                transferAndCommit(session, flowFile, SUCCESS);
                return;
            }

            while (resultIterator.hasNext()) {
                ServiceResult result = resultIterator.next();
                session.append(flowFile, out -> out.write(result.getContent(new BytesHandle()).get()));
            }

            transferAndCommit(session, flowFile, SUCCESS);
        } catch (final Throwable t) {
            this.handleThrowable(t, session);
        }
    }

    protected class ExtensionResourceManager extends ResourceManager {

        protected ExtensionResourceManager(DatabaseClient client, String resourceName) {
            super();
            client.init(resourceName, this);
        }

        protected ServiceResultIterator callService(String method, AbstractWriteHandle writeHandle, RequestParameters parameters) {
            ServiceResultIterator serviceResultIterator;
            switch (method) {
                case MethodTypes.GET_STR:
                    serviceResultIterator = getServices().get(parameters);
                    break;
                case MethodTypes.POST_STR:
                    serviceResultIterator = getServices().post(parameters, writeHandle);
                    break;
                case MethodTypes.PUT_STR:
                    serviceResultIterator = getServices().put(parameters, writeHandle, null);
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

    public static class PayloadSources extends AllowableValuesSet {

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

    public static class MethodTypes extends AllowableValuesSet {

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
