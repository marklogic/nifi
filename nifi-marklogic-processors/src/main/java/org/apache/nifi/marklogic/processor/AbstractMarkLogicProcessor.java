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
import com.marklogic.client.ForbiddenUserException;
import com.marklogic.client.MarkLogicBindingException;
import com.marklogic.client.ResourceNotFoundException;
import com.marklogic.client.UnauthorizedUserException;
import com.marklogic.client.document.ServerTransform;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.marklogic.controller.MarkLogicDatabaseClientService;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
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
        .description("Defines the details for connecting to MarkLogic")
        .identifiesControllerService(MarkLogicDatabaseClientService.class)
        .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Batch Size")
        .displayName("Batch Size")
        .required(true)
        .defaultValue("100")
        .description("The number of documents per batch")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    public static final PropertyDescriptor THREAD_COUNT = new PropertyDescriptor.Builder()
        .name("Thread Count")
        .displayName("Thread Count")
        .required(false)
        .defaultValue("3")
        .description("The number of threads to use for making calls to MarkLogic")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    public static final PropertyDescriptor TRANSFORM = new PropertyDescriptor.Builder()
        .name("Server Transform")
        .displayName("Server Transform")
        .description("The name of a REST server transform to apply to each document; " +
            "see https://docs.marklogic.com/guide/rest-dev/transforms for more information on REST transforms.")
        .addValidator(Validator.VALID)
        .required(false)
        .build();

    protected final Map<String, List<PropertyDescriptor>> propertiesByPrefix = new ConcurrentHashMap<>();

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

    // Memoized here for quick reference via getAttributesToCopy
    private final static List<String> CORE_ATTRIBUTE_KEYS = new ArrayList<>();

    static {
        for (CoreAttributes value : CoreAttributes.values()) {
            CORE_ATTRIBUTE_KEYS.add(value.key());
        }
    }

    @Override
    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(DATABASE_CLIENT_SERVICE);
        list.add(BATCH_SIZE);
        list.add(THREAD_COUNT);
        properties = Collections.unmodifiableList(list);
    }

    /**
     * Populates the {@code propertiesByPrefix} map based on all the properties in the given context that have a
     * colon in their name. The text to the left of the colon is treated as a prefix, and every property starting with
     * that prefix is tossed into a list in the {@code propertiesByPrefix} map. Subclasses can then easily find all
     * properties that start with a particular (and usually known) prefix.
     *
     * @param context
     */
    protected void populatePropertiesByPrefix(ProcessContext context) {
        propertiesByPrefix.clear();
        for (PropertyDescriptor propertyDesc : context.getProperties().keySet()) {
            if (propertyDesc.isDynamic() && propertyDesc.getName().contains(":")) {
                String[] parts = propertyDesc.getName().split(":", 2);
                String prefix = parts[0];
                List<PropertyDescriptor> propertyDescriptors = propertiesByPrefix.get(prefix);
                if (propertyDescriptors == null) {
                    propertyDescriptors = new CopyOnWriteArrayList<>();
                    propertiesByPrefix.put(prefix, propertyDescriptors);
                }
                propertyDescriptors.add(propertyDesc);
            }
        }
    }

    protected DatabaseClient getDatabaseClient(ProcessContext context) {

        PropertyValue mlSvcPropValue = context.getProperty(DATABASE_CLIENT_SERVICE);
        Objects.requireNonNull(mlSvcPropValue, "DATABASE_CLIENT_SERVICE property should not be null");
        MarkLogicDatabaseClientService mlSvc = mlSvcPropValue.asControllerService(MarkLogicDatabaseClientService.class);
        Objects.requireNonNull(mlSvc, "MarkLogicDatabaseClientService should not be null");
        return mlSvc.getDatabaseClient();
    }

    protected String[] getArrayFromCommaSeparatedString(String stringValue) {
        String[] stringArray = null;

        if (stringValue != null && !stringValue.isEmpty()) {
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
        ExpressionLanguageScope scope = ExpressionLanguageScope.ENVIRONMENT;

        switch (prefix) {
            case "trans":
                description = "Defines a parameter named '" + postfix + "' that will be passed to a REST transform";
                break;
            case "ns":
                description = "Maps value to namespace prefix '" + postfix + "'";
                break;
            case "meta":
                description = "Defines a document metadata key with name '" + postfix + "' that will be added to each document";
                scope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
                break;
            case "property":
                description = "Defines a property with name '" + postfix + "' that will be added to the properties fragment of each document";
                scope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
                break;
            default:
                description = "";
                break;
        }

        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .addValidator(Validator.VALID)
            .dynamic(true)
            .expressionLanguageSupported(scope)
            .required(false)
            .description(description)
            .build();
    }

    protected ServerTransform buildServerTransform(ProcessContext context) {
        ServerTransform serverTransform = null;
        Objects.requireNonNull(context.getProperty(TRANSFORM), "TRANSFORM property should not be null");
        final String transform = context.getProperty(TRANSFORM).getValue();
        if (transform != null) {
            serverTransform = new ServerTransform(transform);
            final String transformPrefix = "trans";
            final List<PropertyDescriptor> transformProperties = propertiesByPrefix.get(transformPrefix);
            if (transformProperties != null) {
                for (final PropertyDescriptor descriptor : transformProperties) {
                    String value = getTransformParamValue(context, descriptor);
                    serverTransform.addParameter(descriptor.getName().substring(transformPrefix.length() + 1), value);
                }
            }
        }
        return serverTransform;
    }

    private String getTransformParamValue(ProcessContext context, PropertyDescriptor descriptor) {
        try {
            // During 1.16.3.3 development, found that while this works fine when run in NiFi 1.18 through 1.22, it
            // fails within a NiFi test runner - which perhaps is a bug in NiFi's test runner?
            // The exception is: "Attempting to evaluate expression language for trans:throwErrorForUri using flow file attributes but the scope evaluation is set to VARIABLE_REGISTRY.",
            // which seems erroneous as the descriptor is not evaluated against a FlowFile here.
            // To allow for this to work in a test, a try/catch is used on this exception with the catch block
            // simply not evaluating any expressions.
            Objects.requireNonNull(descriptor, "descriptor should not be null");
            Objects.requireNonNull(context.getProperty(descriptor),
                "property for descriptor " + descriptor.getName() + " should not be null");
            return context.getProperty(descriptor).evaluateAttributeExpressions(context.getAllProperties()).getValue();
        } catch (IllegalStateException ex) {
            getLogger().debug("Unexpected error while getting transform param value: descriptor: " + descriptor + "; error: " + ex.getMessage());
            return context.getProperty(descriptor).getValue();
        }
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
     * Use this for transferring the FlowFile to a Relationship while both logging the error and capturing its
     * message in the FlowFile. This allows the user to do whatever they'd like with the FlowFile after a failure
     * occurs - i.e. they can send it back to the processor for retry, discard it, etc.
     *
     * @param t
     * @param flowFile
     * @param session
     * @param relationship
     */
    protected final void logErrorAndTransfer(Throwable t, FlowFile flowFile, ProcessSession session, Relationship relationship) {
        logError(t);
        addErrorMessageToFlowFile(t.getMessage(), flowFile, session);
        transferAndCommit(session, flowFile, relationship);
    }

    protected final void addErrorMessageToFlowFile(String message, FlowFile flowFile, ProcessSession session) {
        if (message != null) {
            session.putAttribute(flowFile, "markLogicErrorMessage", message);
        }
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
     * The purpose of the synchronized block is to ensure that when a processor instance runs multiple threads - such
     * as a processor that uses QueryBatcher - 2+ threads are not trying to commit or create/modify a FlowFile at the
     * same time with the same session. In that scenario, NiFi will throw an exception like this:
     * "java.lang.IllegalStateException: Cannot commit session while writing to FlowFile". If such a processor
     * calls commit or commitAsync outside of this method, it should use synchronized as well.
     *
     * @param session
     * @param flowFile
     * @param relationship
     */
    protected void transferAndCommit(ProcessSession session, FlowFile flowFile, Relationship relationship) {
        synchronized (session) {
            session.transfer(flowFile, relationship);
            session.commitAsync();
        }
    }

    /**
     * @param incomingFlowFile
     * @return the subset of attributes on the incomingFlowFile that can then be included on new FlowFiles. Testing
     * seems to indicate that NiFi won't allow for filename/uuid/path to be overridden, but to be safe, those 3 are
     * still excluded.
     */
    protected final Map<String, String> getAttributesToCopy(FlowFile incomingFlowFile) {
        Map<String, String> attributes = new HashMap<>();
        incomingFlowFile.getAttributes().forEach((key, value) -> {
            if (!CORE_ATTRIBUTE_KEYS.contains(key)) {
                attributes.put(key, value);
            }
        });
        return attributes;
    }

    /**
     * Helper method for the common use case of creating a new FlowFile and including attributes from another
     * FlowFile - typically the incoming one. This is used instead of {@code session.create(incomingFlowFile)}, as that
     * technique requires the incoming FlowFile to not have been transferred and committed yet. That requirement will
     * cause issues, such as out of memory errors, for any processor that wants to create a large number - around
     * hundreds of thousands or more - of FlowFiles from the incoming FlowFile.
     *
     * Additionally, the technique of creating a new FlowFile - as opposed to using
     * {@code session.create(incomingFlowFile} - seems acceptable per the NiFi javadocs for {@code session.create()},
     * which state "This method is appropriate only when data is received or created from an external system".
     *
     * @param session
     * @param attributes this is expected to have been created via {@code getAttributesToCopy}, which ensures that NiFi
     *                   core attributes are not included
     * @return
     */
    protected final FlowFile createFlowFileWithAttributes(ProcessSession session, Map<String, String> attributes) {
        FlowFile flowFile = session.create();
        if (attributes != null) {
            session.putAllAttributes(flowFile, attributes);
        }
        return flowFile;
    }
}
