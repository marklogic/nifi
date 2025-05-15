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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.datamovement.impl.WriteEventImpl;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * The TriggerWhenEmpty annotation is used so that this processor has a chance to flush the WriteBatcher when no
 * flowfiles are ready to be received.
 */
@Tags({"MarkLogic", "Put", "Write", "Insert"})
@CapabilityDescription("Write batches of FlowFiles as documents to a MarkLogic server using the " +
    "MarkLogic Data Movement SDK (DMSDK). Requires a MarkLogic user with the 'rest-writer' privilege. This processor " +
    "uses NiFi's TriggerWhenEmpty support so that it can run when no upstream FlowFiles are available. This ensures " +
    "that partial batches can be written without having to wait for more FlowFiles. Use NiFi's 'Yield Duration' " +
    "setting to control how often this processor will check for new FlowFiles and flush partial batches.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@DynamicProperties({
    @DynamicProperty(
        name = "trans:{name}",
        value = "The value of a parameter to be passed to a REST server transform",
        description = "A transform parameter with name equal to that of '{name}' will be passed to the REST server " +
            "transform identified by the optional 'Server Transform' property",
        expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT
    ),
    @DynamicProperty(
        name = "property:{name}",
        value = "Value of the document property",
        description = "Defines the name and value of a property to add to the properties fragment of each document",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    ),
    @DynamicProperty(
        name = "meta:{name}",
        value = "Value of the document metadata key",
        description = "Defines the name and value of a document metadata key to add to each document",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
})
@TriggerWhenEmpty
@WritesAttributes(value = {
    @WritesAttribute(attribute = "URIs", description = "On batch_success, writes successful URIs as coma-separated list"),
    @WritesAttribute(attribute = "optionsJson", description = "Deprecated; see https://github.com/marklogic/nifi/issues/194 for more information.")
})
public class PutMarkLogic extends AbstractMarkLogicProcessor {
    class FlowFileInfo {
        FlowFile flowFile;
        ProcessSession session;
        WriteEvent writeEvent;

        FlowFileInfo(FlowFile flowFile, ProcessSession session, WriteEvent writeEvent) {
            this.flowFile = flowFile;
            this.session = session;
            this.writeEvent = writeEvent;
        }
    }

    protected static final Map<String, FlowFileInfo> uriFlowFileMap = new ConcurrentHashMap<>();

    //The map contains the uri/flowfileId
    protected static final Map<String, String> duplicateFlowFileMap = new ConcurrentHashMap<>();

    //Duplicate URI Handling Properties
    public static final String IGNORE = "IGNORE";
    public static final String FAIL_URI = "FAIL_URI";
    public static final String CLOSE_BATCH = "CLOSE_BATCH";

    private static final AllowableValue DUPLICATE_IGNORE = new AllowableValue(IGNORE, IGNORE,
        "No checks will be made for duplicate URIs within a batch");
    private static final AllowableValue DUPLICATE_FAIL_URI = new AllowableValue(FAIL_URI, FAIL_URI,
        "When a duplicate URI within a batch is detected, a FlowFile with the URI will be sent to the 'duplicate_uri' relationship");
    private static final AllowableValue DUPLICATE_CLOSE_BATCH = new AllowableValue(CLOSE_BATCH, CLOSE_BATCH,
        "When a duplicate URI within a batch is detected, the current batch is immediately written to avoid an error");

    public static final PropertyDescriptor COLLECTIONS = new PropertyDescriptor.Builder()
        .name("Collections")
        .displayName("Collections")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .description("Comma-delimited sequence of collections to add to each document")
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor QUALITY = new PropertyDescriptor.Builder()
        .name("Quality")
        .displayName("Quality")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .description("Quality for each document; if not specified, MarkLogic will set default quality 0")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .build();

    public static final PropertyDescriptor FORMAT = new PropertyDescriptor.Builder()
        .name("Format")
        .displayName("Format")
        .description("Format for each document; if not specified, MarkLogic will determine the format" +
            " based on the URI")
        .allowableValues(Format.JSON.name(), Format.XML.name(), Format.TEXT.name(), Format.BINARY.name(), Format.UNKNOWN.name())
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor JOB_ID = new PropertyDescriptor.Builder()
        .name("Job ID")
        .displayName("Job ID")
        .description("ID for the WriteBatcher job")
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor JOB_NAME = new PropertyDescriptor.Builder()
        .name("Job Name")
        .displayName("Job Name")
        .description("Name for the WriteBatcher job")
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor MIMETYPE = new PropertyDescriptor.Builder()
        .name("MIME type")
        .displayName("MIME type")
        .description("MIME type for each document; if not specified, MarkLogic will determine the " +
            "MIME type based on the URI")
        .addValidator(Validator.VALID)
        .required(false)
        .build();

    public static final PropertyDescriptor PERMISSIONS = new PropertyDescriptor.Builder()
        .name("Permissions")
        .displayName("Permissions")
        .defaultValue("rest-reader,read,rest-writer,update")
        .description("Comma-delimited sequence of permissions - role1, capability1, role2, " +
            "capability2 - to add to each document")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(Validator.VALID)
        .required(false)
        .build();

    public static final PropertyDescriptor TEMPORAL_COLLECTION = new PropertyDescriptor.Builder()
        .name("Temporal Collection")
        .displayName("Temporal Collection")
        .description("The temporal collection to use for a temporal document insert")
        .addValidator(Validator.VALID)
        .required(false)
        .build();

    public static final PropertyDescriptor URI_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
        .name("URI Attribute Name")
        .displayName("URI Attribute Name")
        .defaultValue("uuid")
        .required(true)
        .description("The name of the FlowFile attribute whose value will be used as the URI")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor URI_PREFIX = new PropertyDescriptor.Builder()
        .name("URI Prefix")
        .displayName("URI Prefix")
        .description("The prefix to prepend to each URI")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor URI_SUFFIX = new PropertyDescriptor.Builder()
        .name("URI Suffix")
        .displayName("URI Suffix")
        .description("The suffix to append to each URI")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor DUPLICATE_URI_HANDLING = new PropertyDescriptor.Builder()
        .name("Duplicate URI Handling")
        .displayName("Duplicate URI Handling")
        .description("Defines how to handle duplicate URIs within the same batch")
        .required(false)
        .allowableValues(DUPLICATE_IGNORE, DUPLICATE_FAIL_URI, DUPLICATE_CLOSE_BATCH)
        .defaultValue(DUPLICATE_IGNORE.getValue())
        .build();

    public static final PropertyDescriptor RESTART_FAILED_BATCHER = new PropertyDescriptor.Builder()
        .name("Restart Failed Batcher")
        .displayName("Restart Failed Batcher")
        .description("Set to 'true' if the processor should try to restart when the underlying batcher fails. Can " +
            "assist in a scenario where MarkLogic is temporarily unavailable, as the batcher will stop but then start " +
            "back up once it can successfully connect to MarkLogic.")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(false)
        .build();

    protected static final Relationship BATCH_SUCCESS = new Relationship.Builder()
        .name("batch_success")
        .description("A FlowFile is created and written to this relationship for each batch. " +
            "The FlowFile has an attribute of 'URIs', which is a comma-separated list of URIs successfully written in a batch. " +
            "This can assist with post-batch processing")
        .build();

    protected static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles that have been written successfully to MarkLogic are passed to this relationship")
        .build();

    protected static final Relationship FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles that have failed to be written to MarkLogic are passed to this relationship")
        .build();

    protected static final Relationship DUPLICATE_URI = new Relationship.Builder()
        .name("duplicate_uri")
        .description("When 'Duplicate URI Handling' is set to 'FAIL_URI', FlowFiles identifying duplicate URIs will " +
            "be sent to this relationship")
        .build();

    private volatile DataMovementManager dataMovementManager;
    protected volatile WriteBatcher writeBatcher;

    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);

        List<PropertyDescriptor> list = new ArrayList<>();
        list.addAll(properties);
        list.add(COLLECTIONS);
        list.add(QUALITY);
        list.add(FORMAT);
        list.add(JOB_ID);
        list.add(JOB_NAME);
        list.add(MIMETYPE);
        list.add(PERMISSIONS);
        list.add(TRANSFORM);
        list.add(TEMPORAL_COLLECTION);
        list.add(URI_ATTRIBUTE_NAME);
        list.add(URI_PREFIX);
        list.add(URI_SUFFIX);
        list.add(DUPLICATE_URI_HANDLING);
        list.add(RESTART_FAILED_BATCHER);
        properties = Collections.unmodifiableList(list);

        Set<Relationship> set = new HashSet<>();
        set.add(BATCH_SUCCESS);
        set.add(SUCCESS);
        set.add(FAILURE);
        set.add(DUPLICATE_URI);
        relationships = Collections.unmodifiableSet(set);
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        getLogger().info("OnScheduled");
        super.populatePropertiesByPrefix(context);
        dataMovementManager = getDatabaseClient(context).newDataMovementManager();
        createAndStartWriteBatcher(context);
    }

    private void createAndStartWriteBatcher(ProcessContext context) {
        DatabaseClient client = getDatabaseClient(context);
        DatabaseClient.ConnectionResult result = client.checkConnection();
        if (!result.isConnected()) {
            throw new ProcessException("Unable to connect to MarkLogic; cause: " + result.getErrorMessage());
        }

        PropertyValue jobIdProp = context.getProperty(JOB_ID);
        Objects.requireNonNull(jobIdProp);

        PropertyValue jobNameProp = context.getProperty(JOB_NAME);
        Objects.requireNonNull(jobNameProp);

        PropertyValue batchSizeProp = context.getProperty(BATCH_SIZE);
        Objects.requireNonNull(batchSizeProp);

        PropertyValue temporalCollectionsProp = context.getProperty(TEMPORAL_COLLECTION);
        Objects.requireNonNull(temporalCollectionsProp);

        PropertyValue threadCountProp = context.getProperty(THREAD_COUNT);
        Objects.requireNonNull(threadCountProp);

        try {
            this.writeBatcher = dataMovementManager.newWriteBatcher();
            Objects.requireNonNull(this.writeBatcher);
            this.writeBatcher
                .withJobId(jobIdProp.getValue())
                .withJobName(jobNameProp.getValue())
                .withBatchSize(batchSizeProp.asInteger())
                .withTemporalCollection(temporalCollectionsProp.getValue());

            ServerTransform serverTransform = buildServerTransform(context);
            if (serverTransform != null) {
                writeBatcher.withTransform(serverTransform);
            }
            Integer threadCount = threadCountProp.asInteger();
            if (threadCount != null) {
                writeBatcher.withThreadCount(threadCount);
            }
            this.writeBatcher.onBatchSuccess(writeBatch -> {
                if (writeBatch.getItems().length > 0) {
                    FlowFileInfo flowFileInfo = getFlowFileInfoForWriteEvent(writeBatch.getItems()[0]);
                    if (flowFileInfo != null) {
                        ProcessSession session = flowFileInfo.session;
                        String uriList = Stream.of(writeBatch.getItems()).map(WriteEvent::getTargetUri).collect(Collectors.joining(","));
                        FlowFile batchFlowFile = session.create();
                        session.putAttribute(batchFlowFile, "URIs", uriList);
                        addDeprecatedOptionsJsonAttribute(session, batchFlowFile, uriList);
                        synchronized (session) {
                            session.transfer(batchFlowFile, BATCH_SUCCESS);
                        }
                    }
                    for (WriteEvent writeEvent : writeBatch.getItems()) {
                        transferFlowFile(writeEvent, SUCCESS);
                        duplicateFlowFileMap.remove(writeEvent.getTargetUri());
                    }
                }
            }).onBatchFailure((writeBatch, throwable) -> {
                for (WriteEvent writeEvent : writeBatch.getItems()) {
                    transferFlowFile(writeEvent, FAILURE);
                    duplicateFlowFileMap.remove(writeEvent.getTargetUri());
                }
            });
        } catch (Exception ex) {
            throw new RuntimeException("Unable to create WriteBatcher, cause: " + ex.getMessage(), ex);
        }

        dataMovementManager.startJob(writeBatcher);
    }

    /**
     * "optionsJson" was added by the marklogic-nifi-incubator project. The intent was to pass it to an internal DHF
     * endpoint, but that's not supported. It should be removed in the next major release.
     * https://github.com/marklogic/nifi/issues/194 shows an example of how to construct a DHF sourceQuery using the
     * documented "URIs" attribute.
     *
     * @param batchFlowFile
     * @param uriList
     */
    @Deprecated
    private void addDeprecatedOptionsJsonAttribute(ProcessSession session, FlowFile batchFlowFile, String uriList) {
        JsonObject optionsJSONObj = new JsonObject();
        JsonArray jsonArray = new JsonArray();
        for (String uri : uriList.split(",")) {
            jsonArray.add(uri);
        }
        optionsJSONObj.add("uris", jsonArray);
        session.putAttribute(batchFlowFile, "optionsJson", optionsJSONObj.toString());
    }

    protected FlowFileInfo getFlowFileInfoForWriteEvent(WriteEvent writeEvent) {
        DocumentMetadataHandle metadata = (DocumentMetadataHandle) writeEvent.getMetadata();
        String flowFileUUID = metadata.getMetadataValues().get("flowFileUUID");
        return uriFlowFileMap.get(flowFileUUID);
    }

    /**
     * Convenience method for transferring the FlowFile associated with the given WriteEvent to the given Relationship.
     * This depends on an entry existing in the underlying uriFlowFileMap map that is associated with the given
     * WriteEvent. If no entry is found, no action can be taken - though this is not expected as the only way an entry
     * is removed from that map is via this method.
     *
     * @param writeEvent
     * @param relationship
     */
    protected void transferFlowFile(WriteEvent writeEvent, Relationship relationship) {
        FlowFileInfo flowFile = getFlowFileInfoForWriteEvent(writeEvent);
        if (flowFile != null) {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Routing " + writeEvent.getTargetUri() + " to " + relationship.getName());
            }
            synchronized (flowFile.session) {
                flowFile.session.getProvenanceReporter().send(flowFile.flowFile, writeEvent.getTargetUri());
                flowFile.session.transfer(flowFile.flowFile, relationship);
                flowFile.session.commitAsync();
            }
            uriFlowFileMap.remove(flowFile.flowFile.getAttribute(CoreAttributes.UUID.key()));
        }

    }

    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        onTrigger(context, session);
    }

    /**
     * When a FlowFile is received, hand it off to the WriteBatcher so that it can be written to MarkLogic.
     * <p>
     * If a FlowFile is not set (possible because of the TriggerWhenEmpty annotation), then yield is called on the
     * ProcessContext so that Nifi doesn't invoke this method repeatedly when nothing is available. The WriteBatcher
     * is flushed asynchronously as well, ensuring that any batch of document that is smaller than the WriteBatcher's
     * batch size will be flushed immediately and not have to wait for more FlowFiles to arrive to fill out the batch.
     */
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            getLogger().debug("Flushing the WriteBatcher asynchronously in case a number of documents less than batchSize are waiting to be written");
            flushWriteBatcherAsync(this.writeBatcher);
            getLogger().debug("Calling yield() on the ProcessContext");
            context.yield();
            return;
        }

        if (this.writeBatcher != null && this.writeBatcher.isStopped()) {
            PropertyValue restartFailedBatcherProp = context.getProperty(RESTART_FAILED_BATCHER);
            Objects.requireNonNull(restartFailedBatcherProp);
            if ("true".equals(restartFailedBatcherProp.getValue())) {
                // Ensure that if the process has multiple tasks assigned to it, only one triggers this block.
                synchronized (this.dataMovementManager) {
                    if (this.writeBatcher.isStopped()) {
                        getLogger().info("Batcher is stopped; attempting to create and start a new batcher.");
                        if (!createAndStartNewBatcher(flowFile, context, session)) {
                            // Delay scheduling the processor to give MarkLogic time to become available again before we
                            // try to start a new batcher again.
                            getLogger().info("Couldn't start new batcher, so going to yield.");
                            context.yield();
                            return;
                        }
                    }
                }
            } else {
                addErrorMessageToFlowFile("The batcher is stopped.", flowFile, session);
                transferAndCommit(session, flowFile, FAILURE);
                context.yield();
                return;
            }
        }

        try {
            PropertyValue duplicateUriHandlingProp = context.getProperty(DUPLICATE_URI_HANDLING);
            Objects.requireNonNull(duplicateUriHandlingProp);
            String duplicateHandler = duplicateUriHandlingProp.getValue();

            WriteEvent writeEvent = buildWriteEvent(context, session, flowFile);

            String currentUrl = writeEvent.getTargetUri();
            String currentUUID = flowFile.getAttribute(CoreAttributes.UUID.key());
            String previousUUID = duplicateFlowFileMap.get(currentUrl);

            //Looks like the best place to detect duplicates and handle action because we have access to computed url by this point,
            switch (duplicateHandler) {
                case IGNORE:
                    //Just write the event knowing it will fail during batch write process
                    uriFlowFileMap.put(currentUUID, new FlowFileInfo(flowFile, session, writeEvent));
                    addWriteEvent(this.writeBatcher, writeEvent);
                    break;
                case FAIL_URI:
                    if (previousUUID != null && !previousUUID.equals(currentUUID)) {
                        uriFlowFileMap.put(currentUUID, new FlowFileInfo(flowFile, session, writeEvent));
                        transferFlowFile(writeEvent, DUPLICATE_URI);

                    } else {
                        uriFlowFileMap.put(currentUUID, new FlowFileInfo(flowFile, session, writeEvent));
                        duplicateFlowFileMap.put(currentUrl, currentUUID);
                        addWriteEvent(this.writeBatcher, writeEvent);
                    }
                    break;

                case CLOSE_BATCH:
                    if (previousUUID != null) {
                        getLogger().info("Closing batch; duplicate URI:" + writeEvent.getTargetUri());
                        this.flushAndWait();
                    }
                    uriFlowFileMap.put(currentUUID, new FlowFileInfo(flowFile, session, writeEvent));
                    duplicateFlowFileMap.put(currentUrl, currentUUID);
                    addWriteEvent(this.writeBatcher, writeEvent);
                    break;
            }
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Writing URI: " + writeEvent.getTargetUri());
            }
        } catch (IllegalStateException ex) {
            // An ISE is most likely to occur due to the WriteBatcher having stopped. In that scenario, we don't
            // need a stacktrace logged. Just need to send the failed FlowFile to the FAILURE relationship.
            addErrorMessageToFlowFile(ex.getMessage(), flowFile, session);
            transferAndCommit(session, flowFile, FAILURE);
        } catch (final Throwable t) {
            // Catches any exception that occurs outside of writing a batch. We don't have a way of reproducing
            // this in a test as there's not a way to force an error outside of writing a batch. So exceptions
            // here will be rare and unexpected, but still need to route them to the failure relationship.
            logErrorAndTransfer(t, flowFile, session, FAILURE);
        }
    }

    /*
     * Protected so that it can be overridden for unit testing purposes.
     */
    protected void flushWriteBatcherAsync(WriteBatcher writeBatcher) {
        flushAsyncWithoutFailing(writeBatcher);
    }

    /*
     * Protected so that it can be overridden for unit testing purposes.
     */
    protected void addWriteEvent(WriteBatcher writeBatcher, WriteEvent writeEvent) {
        writeBatcher.add(writeEvent);
    }

    protected WriteEvent buildWriteEvent(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        PropertyValue uriAttributeNameProp = context.getProperty(URI_ATTRIBUTE_NAME);
        Objects.requireNonNull(uriAttributeNameProp);
        String uri = flowFile.getAttribute(uriAttributeNameProp.getValue());

        PropertyValue uriPrefixProp = context.getProperty(URI_PREFIX);
        Objects.requireNonNull(uriPrefixProp);
        final String prefix = uriPrefixProp.evaluateAttributeExpressions(flowFile).getValue();
        if (prefix != null) {
            uri = prefix + uri;
        }

        PropertyValue uriSuffixProp = context.getProperty(URI_SUFFIX);
        Objects.requireNonNull(uriSuffixProp);
        final String suffix = uriSuffixProp.evaluateAttributeExpressions(flowFile).getValue();
        if (suffix != null) {
            uri += suffix;
        }

        PropertyValue collectionsProp = context.getProperty(COLLECTIONS);
        Objects.requireNonNull(collectionsProp);
        PropertyValue permissionsProp = context.getProperty(PERMISSIONS);
        Objects.requireNonNull(permissionsProp);
        DocumentMetadataHandle metadata = buildMetadataHandle(context, flowFile, collectionsProp, permissionsProp);
        final byte[] content = new byte[(int) flowFile.getSize()];
        session.read(flowFile, inputStream -> StreamUtils.fillBuffer(inputStream, content));

        BytesHandle handle = new BytesHandle(content);

        PropertyValue formatProp = context.getProperty(FORMAT);
        Objects.requireNonNull(formatProp);
        final String format = formatProp.getValue();
        if (format != null) {
            handle.withFormat(Format.valueOf(format));
        } else {
            addFormat(uri, handle);
        }

        PropertyValue mimeTypeProp = context.getProperty(MIMETYPE);
        Objects.requireNonNull(mimeTypeProp);
        final String mimetype = mimeTypeProp.getValue();
        if (mimetype != null) {
            handle.withMimetype(mimetype);
        }

        return new WriteEventImpl()
            .withTargetUri(uri)
            .withMetadata(metadata)
            .withContent(handle);
    }

    protected DocumentMetadataHandle buildMetadataHandle(
        final ProcessContext context,
        final FlowFile flowFile,
        final PropertyValue collectionProperty,
        final PropertyValue permissionsProperty
    ) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();

        // Get collections from processor property definition
        final String collectionsValue = collectionProperty.isSet()
            ? collectionProperty.evaluateAttributeExpressions(flowFile).getValue() : null;

        final String[] collections = getArrayFromCommaSeparatedString(collectionsValue);
        metadata.withCollections(collections);

        // Get permission from processor property definition
        final String permissionsValue = permissionsProperty.isSet() ?
            permissionsProperty.evaluateAttributeExpressions(flowFile).getValue() : null;
        final String[] tokens = getArrayFromCommaSeparatedString(permissionsValue);

        PropertyValue qualityProp = context.getProperty(QUALITY);
        Objects.requireNonNull(qualityProp);
        Integer quality = qualityProp.evaluateAttributeExpressions(flowFile).asInteger();
        if (quality != null) {
            metadata.withQuality(quality);
        }

        if (tokens != null) {
            DocumentMetadataHandle.DocumentPermissions permissions = metadata.getPermissions();
            for (int i = 0; i < tokens.length; i += 2) {
                String role = tokens[i];
                DocumentMetadataHandle.Capability capability = DocumentMetadataHandle.Capability.getValueOf(tokens[i + 1]);
                if (permissions.containsKey(role)) {
                    permissions.get(role).add(capability);
                } else {
                    permissions.add(role, capability);
                }
            }
        }
        String flowFileUUID = flowFile.getAttribute(CoreAttributes.UUID.key());
        // Add the flow file UUID for Provenance purposes and for sending them
        // to the appropriate relationship
        metadata.withMetadataValue("flowFileUUID", flowFileUUID);

        // Set dynamic meta
        String metaPrefix = "meta";
        List<PropertyDescriptor> metaProperties = propertiesByPrefix.get(metaPrefix);
        if (metaProperties != null) {
            for (final PropertyDescriptor propertyDesc : metaProperties) {
                PropertyValue prop = context.getProperty(propertyDesc);
                Objects.requireNonNull(prop);
                metadata.withMetadataValue(propertyDesc.getName().substring(metaPrefix.length() + 1),
                    prop.evaluateAttributeExpressions(flowFile).getValue());
            }
        }
        // Set dynamic properties
        String propertyPrefix = "property";
        List<PropertyDescriptor> propertyProperties = propertiesByPrefix.get(propertyPrefix);
        if (propertyProperties != null) {
            for (final PropertyDescriptor propertyDesc : propertiesByPrefix.get(propertyPrefix)) {
                PropertyValue prop = context.getProperty(propertyDesc);
                Objects.requireNonNull(prop);
                metadata.withProperty(propertyDesc.getName().substring(propertyPrefix.length() + 1),
                    prop.evaluateAttributeExpressions(flowFile).getValue());
            }
        }

        return metadata;
    }

    protected void addFormat(String uri, BytesHandle handle) {
        int extensionStartIndex = uri.lastIndexOf(".");
        if (extensionStartIndex > 0) {
            String extension = uri.substring(extensionStartIndex + 1).toLowerCase();
            switch (extension) {
                case "xml":
                    handle.withFormat(Format.XML);
                    break;
                case "json":
                    handle.withFormat(Format.JSON);
                    break;
                case "txt":
                    handle.withFormat(Format.TEXT);
                    break;
                default:
                    handle.withFormat(Format.UNKNOWN);
                    break;
            }
        }
    }

    // Protected instead of private so that it can be overridden for testing purposes
    protected void flushAndWait() {
        flushAndWaitWithoutFailing(writeBatcher);
    }

    @OnShutdown
    public void onShutdown() {
        getLogger().info("OnShutdown");
        completeWriteBatcherJob();
    }

    @OnStopped
    public void onStopped() {
        getLogger().info("OnStopped");
        completeWriteBatcherJob();
    }

    @OnUnscheduled
    public void onUnscheduled() {
        getLogger().info("OnUnscheduled");
        completeWriteBatcherJob();
    }

    /**
     * Synchronizing this as of 1.24.1. Testing shows that even with more than 1 task assigned to an instance of this
     * processor, this method should only be invoked once by NiFi. So synchronizing should not have an impact - but
     * if it is in fact possible for two or more threads to invoke this by NiFi, this will ensure we do not have race
     * conditions between those two threads.
     */
    private synchronized void completeWriteBatcherJob() {
        if (writeBatcher != null) {
            getLogger().info("Calling flushAndWait on WriteBatcher");
            flushAndWaitWithoutFailing(writeBatcher);
            getLogger().info("Awaiting completion");
            writeBatcher.awaitCompletion();
            getLogger().info("Stopping WriteBatcher job");
            dataMovementManager.stopJob(writeBatcher);
            getLogger().info("WriteBatcher job stopped");
        }
        writeBatcher = null;
        dataMovementManager = null;
    }

    /**
     * Added in 1.24.1 in response to https://github.com/marklogic/nifi/issues/217 , where a call is made to flush
     * the batcher when it has been stopped for unknown reasons.
     */
    protected final void flushAndWaitWithoutFailing(WriteBatcher batcher) {
        if (batcher != null) {
            if (batcher.isStopped()) {
                return;
            }
            try {
                batcher.flushAndWait();
            } catch (IllegalStateException e) {
                getLogger().error("Received error while synchronously flushing batch: {}", e.getMessage());
            }
        }
    }

    /**
     * Added in 1.24.1 in response to https://github.com/marklogic/nifi/issues/217 , where a call is made to flush
     * the batcher when it has been stopped for unknown reasons.
     */
    protected final void flushAsyncWithoutFailing(WriteBatcher batcher) {
        if (batcher != null) {
            if (batcher.isStopped()) {
                return;
            }
            try {
                batcher.flushAsync();
            } catch (IllegalStateException e) {
                getLogger().error("Received error while asynchronously flushing batch: {}", e.getMessage());
            }
        }
    }

    /**
     * If the batcher is stopped - such as by the DMSDK HostAvailabilityListener - then the processor is effectively
     * useless, even if MarkLogic becomes available again. That is due to a batcher not being usable once it has been
     * stopped. This method then attempts to create and start a new batcher which allows the process to resume its
     * normal operation. The method is synchronized to ensure that if the user has 2 or more tasks assigned to this
     * processor, only one of them will attempt to do this (the caller of this method helps with that too).
     */
    private synchronized boolean createAndStartNewBatcher(FlowFile flowFile, ProcessContext context, ProcessSession session) {
        getLogger().info("Attempting to create and start new batcher.");
        try {
            createAndStartWriteBatcher(context);
            return true;
        } catch (Exception e) {
            String message = "Unable to create and start new batcher, cause: " + e.getMessage();
            getLogger().error(message);
            addErrorMessageToFlowFile(message, flowFile, session);
            transferAndCommit(session, flowFile, FAILURE);
            return false;
        }
    }
}
