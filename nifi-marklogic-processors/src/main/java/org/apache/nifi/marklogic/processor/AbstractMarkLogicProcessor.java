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

import com.marklogic.client.*;
import com.marklogic.client.document.ServerTransform;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.marklogic.controller.MarkLogicDatabaseClientService;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

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

    /**
     * Use this when you want an error to result in the session being rolled back and a ProcessException being
     * thrown. Note that this should result in an incoming FlowFile being left in the queue before the processor. This
     * may not be desirable to a user; it may be better to route the FlowFile to a failure relationship.
     *
     * @param t
     * @param session
     */
    protected void logErrorAndRollbackSession(final Throwable t, final ProcessSession session) {
        logError(t);
        getLogger().info("Rolling back session");
        session.rollback(true);
        throw new ProcessException(t);
    }

    protected void logError(Throwable t) {
        final String errorMessage = t.getMessage() != null ? t.getMessage() : "";
        if (t instanceof UnauthorizedUserException || errorMessage.matches(unauthorizedPattern.pattern())) {
            getLogger().error("Verify your credentials are correct; error: " + errorMessage, t);
        } else if (t instanceof ForbiddenUserException || errorMessage.matches(forbiddenPattern.pattern())) {
            getLogger().error("Verify your user has ample privileges; error: " + errorMessage, t);
        } else if (t instanceof ResourceNotFoundException || errorMessage.matches(resourceNotFoundPattern.pattern())) {
            getLogger().error("Failed due to 'Resource Not Found'; " +
                    "verify you're pointing a MarkLogic REST instance and referenced extensions/transforms are installed; error: " + errorMessage, t);
        } else if (errorMessage.matches(invalidXMLPattern.pattern())) {
            getLogger().error("Expected valid XML payload; error: " + errorMessage, t);
        } else if (errorMessage.matches(invalidJSONPattern.pattern())) {
            getLogger().error("Expected valid JSON payload; error: " + errorMessage, t);
        } else if (t instanceof MarkLogicBindingException) {
            getLogger().error("Failed to bind Java Object to XML or JSON; error: " + errorMessage, t);
        } else {
            getLogger().error(errorMessage, t);
        }
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
