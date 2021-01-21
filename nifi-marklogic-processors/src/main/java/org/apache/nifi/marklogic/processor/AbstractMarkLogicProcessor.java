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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.marklogic.controller.MarkLogicDatabaseClientService;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.ForbiddenUserException;
import com.marklogic.client.MarkLogicBindingException;
import com.marklogic.client.ResourceNotFoundException;
import com.marklogic.client.UnauthorizedUserException;
import com.marklogic.client.document.ServerTransform;

/**
 * Defines common properties for MarkLogic processors.
 */
public abstract class AbstractMarkLogicProcessor extends AbstractSessionFactoryProcessor {

    protected List<PropertyDescriptor> properties;
    protected Set<Relationship> relationships;

    public static final PropertyDescriptor DATABASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("DatabaseClient Service")
        .displayName("DatabaseClient Service")
        .required(true)
        .description("The DatabaseClient Controller Service that provides the MarkLogic connection")
        .identifiesControllerService(MarkLogicDatabaseClientService.class)
        .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Batch Size")
        .displayName("Batch Size")
        .required(true)
        .defaultValue("100")
        .description("The number of documents per batch - sets the batch size on the Batcher")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    public static final PropertyDescriptor THREAD_COUNT = new PropertyDescriptor.Builder()
        .name("Thread Count")
        .displayName("Thread Count")
        .required(false)
        .defaultValue("3")
        .description("The number of threads - sets the thread count on the Batcher")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    public static final PropertyDescriptor TRANSFORM = new PropertyDescriptor.Builder()
        .name("Server Transform")
        .displayName("Server Transform")
        .description("The name of REST server transform to apply to every document")
        .addValidator(Validator.VALID)
        .required(false)
        .build();

    protected final Map<String, List<PropertyDescriptor>> propertiesByPrefix = new ConcurrentHashMap<String, List<PropertyDescriptor>>();

    // Patterns for more friendly error messages.
    private Pattern unauthorizedPattern =
            Pattern.compile("(?i)unauthorized", Pattern.CASE_INSENSITIVE);
    private Pattern forbiddenPattern =
            Pattern.compile("(?i)forbidden", Pattern.CASE_INSENSITIVE);
    private Pattern resourceNotFoundPattern =
            Pattern.compile("(?i)resource not found", Pattern.CASE_INSENSITIVE);
    private Pattern invalidXMLPattern =
            Pattern.compile("(?i)XDMP-DOC.*:\\s+xdmp:get-request-body(\"xml\")", Pattern.CASE_INSENSITIVE);
    private Pattern invalidJSONPattern =
            Pattern.compile("(?i)XDMP-DOC.*:\\s+xdmp:get-request-body(\"json\")", Pattern.CASE_INSENSITIVE);

    @Override
    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(DATABASE_CLIENT_SERVICE);
        list.add(BATCH_SIZE);
        list.add(THREAD_COUNT);
        properties = Collections.unmodifiableList(list);
    }

    public void populatePropertiesByPrefix(ProcessContext context) {
        propertiesByPrefix.clear();
        for (PropertyDescriptor propertyDesc: context.getProperties().keySet()) {
            if (propertyDesc.isDynamic() && propertyDesc.getName().contains(":")) {
                String[] parts = propertyDesc.getName().split(":", 2);
                String prefix = parts[0];
                List<PropertyDescriptor> propertyDescriptors = propertiesByPrefix.get(prefix);
                if (propertyDescriptors == null) {
                    propertyDescriptors = new CopyOnWriteArrayList<PropertyDescriptor>();
                    propertiesByPrefix.put(prefix, propertyDescriptors);
                }
                propertyDescriptors.add(propertyDesc);
            }
        }
    }

    protected DatabaseClient getDatabaseClient(ProcessContext context) {
        return context.getProperty(DATABASE_CLIENT_SERVICE)
            .asControllerService(MarkLogicDatabaseClientService.class)
            .getDatabaseClient();
    }

    protected String[] getArrayFromCommaSeparatedString(String stringValue) {
        String[] stringArray = null;

        if (stringValue != null && !stringValue.isEmpty()){
            stringValue = stringValue.trim();

            if (!stringValue.isEmpty()) {
                stringArray = stringValue.split("\\s*,\\s*");
            }
        }

        return stringArray;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        String[] parts = propertyDescriptorName.split(":", 2);
        String prefix = parts[0];
        String postfix = (parts.length > 1) ? parts[1] : "";
        String description;

        switch(prefix) {
            case "trans":
                description = "Passes the parameter '" + propertyDescriptorName + "' for transform";
                break;
            case "ns":
                description = "Maps value to namespace prefix '" + postfix + "'";
                break;
            case "meta":
                description = "Adds the value as the metadata '" + postfix + "'";
                break;
            case "property":
                description = "Adds the value as the property '" + postfix + "'";
                break;
            default:
                description = "";
                break;
        }

        PropertyDescriptor propertyDesc = new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .addValidator(Validator.VALID)
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .description(description)
            .build();
        return propertyDesc;
    }

    protected ServerTransform buildServerTransform(ProcessContext context) {
        ServerTransform serverTransform = null;
        final String transform = context.getProperty(TRANSFORM).getValue();
        if (transform != null) {
            serverTransform = new ServerTransform(transform);
            final String transformPrefix = "trans";
            final List<PropertyDescriptor> transformProperties = propertiesByPrefix.get(transformPrefix);
            if (transformProperties != null) {
                for (final PropertyDescriptor descriptor : transformProperties) {
                    serverTransform.addParameter(
                        descriptor.getName().substring(transformPrefix.length() + 1),
                        context.getProperty(descriptor).evaluateAttributeExpressions(context.getAllProperties()).getValue()
                     );
                }
            }
        }
        return serverTransform;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    public static abstract class AllowableValuesSet {
        public static AllowableValue[] allValues;

    }

    protected void handleThrowable(final Throwable t, final ProcessSession session) {
        final String statusMsg;
        /* FailedRequestException can hide the root cause a layer deeper.
         * Go to the failed request to get the status message.
         */
        if (t instanceof FailedRequestException) {
            statusMsg = ((FailedRequestException) t).getFailedRequest().getMessage();
        } else {
            statusMsg = "";
        }
        final Throwable rootCause = ExceptionUtils.getRootCause(t);
        if (t instanceof UnauthorizedUserException || statusMsg.matches(unauthorizedPattern.pattern())) {
            getLogger().error("{} failed! Verify your credentials are correct. Throwable exception {}; rolling back session", new Object[]{this, rootCause});
        } else if (t instanceof ForbiddenUserException || statusMsg.matches(forbiddenPattern.pattern())) {
            getLogger().error("{} failed! Verify your user has ample privileges. Throwable exception {}; rolling back session", new Object[]{this, rootCause});
        } else if (t instanceof ResourceNotFoundException || statusMsg.matches(resourceNotFoundPattern.pattern())) {
            getLogger().error("{} failed due to 'Resource Not Found'! " +
                    "Verify you're pointing a MarkLogic REST instance and referenced extensions/transforms are installed. "+
                    "Throwable exception {}; rolling back session", new Object[]{this, rootCause});
        } else if (statusMsg.matches(invalidXMLPattern.pattern())) {
            getLogger().error("{} failed! Expected valid XML payload! Throwable exception {}; rolling back session", new Object[]{this, rootCause});
        } else if (statusMsg.matches(invalidJSONPattern.pattern())) {
            getLogger().error("{} failed! Expected valid JSON payload! Throwable exception {}; rolling back session", new Object[]{this, rootCause});
        } else if (t instanceof MarkLogicBindingException) {
            getLogger().error("{} failed to bind Java Object to XML or JSON! Throwable exception {}; rolling back session", new Object[]{this, rootCause});
        } else {
            getLogger().error("{} failed! Throwable exception {}; rolling back session", new Object[]{this, rootCause});
        }
        session.rollback(true);
        throw new ProcessException(rootCause);
    }

    /**
     * See https://github.com/marklogic/nifi/issues/21 for details on why the calls to transfer and commit are within
     * a synchronized block.
     *
     * @param session
     * @param flowFile
     * @param relationship
     */
    protected void transferAndCommit(ProcessSession session, FlowFile flowFile, Relationship relationship) {
        synchronized (session) {
            session.transfer(flowFile, relationship);
            session.commit();
        }
    }
}
