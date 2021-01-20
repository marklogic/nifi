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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.json.JSONObject;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.datamovement.impl.WriteEventImpl;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;


/**
 * The TriggerWhenEmpty annotation is used so that this processor has a chance to flush the WriteBatcher when no
 * flowfiles are ready to be received.
 */
@Tags({"MarkLogic", "Put", "Write", "Insert"})
@CapabilityDescription("Write batches of FlowFiles as documents to a MarkLogic server using the " +
    "MarkLogic Data Movement SDK (DMSDK)")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@DynamicProperty(name = "trans: Server transform parameter name, property: Property name to add, meta: Metadata name to add",
    value = "trans: Value of the server transform parameter, property: Property value to add, meta: Metadata value to add",
    description = "Depending on the property prefix, routes data to transform, metadata, or property.",
    expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
@TriggerWhenEmpty
@WritesAttribute(attribute = "URIs", description = "On batch_success, writes successful URIs as coma-separated list.")

public class PutMarkLogic extends AbstractMarkLogicProcessor {
    class FlowFileInfo {
        FlowFile flowFile;
        ProcessSession session;
        WriteEvent writeEvent;
        FlowFileInfo(FlowFile flowFile, ProcessSession session,WriteEvent writeEvent) {
            this.flowFile = flowFile;
            this.session = session;
            this.writeEvent = writeEvent;
        }
    }
    
    protected static final Map<String, FlowFileInfo> uriFlowFileMap = new ConcurrentHashMap<>();
    
    //The map contains the uri/flowfileId
    protected static final Map<String,String> duplicateFlowFileMap = new ConcurrentHashMap<>();
    
    //Duplicate URI Handling Properties
    public static final String IGNORE      = "IGNORE";
    public static final String FAIL_URI  = "FAIL_URI";
    public static final String CLOSE_BATCH = "CLOSE_BATCH";
    protected static final AllowableValue DUPLICATE_IGNORE = new AllowableValue(IGNORE, IGNORE, "Does not handle duplicate uris");
    protected static final AllowableValue DUPLICATE_FAIL_URI = new AllowableValue(FAIL_URI, FAIL_URI, "Routes a duplicate FlowFile like taking first flow file with targetUri (FIFO)");
    protected static final AllowableValue DUPLICATE_CLOSE_BATCH = new AllowableValue(CLOSE_BATCH,CLOSE_BATCH,"Attempts to close the current batch, before inserting duplicate flowFile");
    
    public static final PropertyDescriptor COLLECTIONS = new PropertyDescriptor.Builder()
        .name("Collections")
        .displayName("Collections")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .description("Comma-delimited sequence of collections to add to each document")
        .required(false)
        .addValidator(Validator.VALID)
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
        .displayName("Duplicate Uri Handling")
        .description("Strategy used for multiple docuuments with same URI")
        .required(false)
        .allowableValues(DUPLICATE_IGNORE,DUPLICATE_FAIL_URI,DUPLICATE_CLOSE_BATCH)
        .defaultValue(DUPLICATE_IGNORE.getValue())
        .build();
    
    protected static final Relationship BATCH_SUCCESS = new Relationship.Builder()
        .name("batch_success")
        .description("All successful URIs in a batch passed comma-separated in URIs FlowFile attribute.")
        .build();

    protected static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles that are successfully written to MarkLogic are routed to the " +
            "success relationship for future processing.")
        .build();

    protected static final Relationship FAILURE = new Relationship.Builder()
        .name("failure")
        .description("All FlowFiles that failed to be written to MarkLogic are routed to the " +
            "failure relationship for future processing.")
        .build();
    
    protected static final Relationship DUPLICATE_URI = new Relationship.Builder()
        .name("duplicate_uri")
        .description("A flowFile that resulted in a DUPLICATE_URI used in FAIL_URI Duplicate Uri Handling")
        .build();
    
    private volatile DataMovementManager dataMovementManager;
    protected volatile WriteBatcher writeBatcher;
    // If no FlowFile exists when this processor is triggered, this variable determines whether or not a call is made to
    // flush the WriteBatcher
    private volatile boolean shouldFlushIfEmpty = true;
    

    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);

        List<PropertyDescriptor> list = new ArrayList<>();
        list.addAll(properties);
        list.add(COLLECTIONS);
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
        properties = Collections.unmodifiableList(list);
        Set<Relationship> set = new HashSet<>();
        set.add(BATCH_SUCCESS);
        set.add(SUCCESS);
        set.add(FAILURE);
        set.add(DUPLICATE_URI);
        //set.add(SUPERSEDED_URI);
        relationships = Collections.unmodifiableSet(set);
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        getLogger().info("OnScheduled");
        super.populatePropertiesByPrefix(context);
        dataMovementManager = getDatabaseClient(context).newDataMovementManager();
        writeBatcher = dataMovementManager.newWriteBatcher()
            .withJobId(context.getProperty(JOB_ID).getValue())
            .withJobName(context.getProperty(JOB_NAME).getValue())
            .withBatchSize(context.getProperty(BATCH_SIZE).asInteger())
            .withTemporalCollection(context.getProperty(TEMPORAL_COLLECTION).getValue());

        ServerTransform serverTransform = buildServerTransform(context);
        if (serverTransform != null) {
            writeBatcher.withTransform(serverTransform);
        }
        Integer threadCount = context.getProperty(THREAD_COUNT).asInteger();
        if(threadCount != null) {
            writeBatcher.withThreadCount(threadCount);
        }
        this.writeBatcher.onBatchSuccess(writeBatch -> {
            if (writeBatch.getItems().length > 0) {
                ProcessSession session = getFlowFileInfoForWriteEvent(writeBatch.getItems()[0]).session;
                String uriList = Stream.of(writeBatch.getItems()).map((item) -> {
                    return item.getTargetUri();
                }).collect(Collectors.joining(","));
                FlowFile batchFlowFile = session.create();
        	    JSONObject optionsJSONObj = new JSONObject();
        	    optionsJSONObj.put("uris", uriList.split(","));        	    
                session.putAttribute(batchFlowFile, "URIs", uriList);
                session.putAttribute(batchFlowFile, "optionsJson", optionsJSONObj.toString());
                synchronized(session) {
                    session.transfer(batchFlowFile, BATCH_SUCCESS);
                }
                for(WriteEvent writeEvent : writeBatch.getItems()) {
                    routeDocumentToRelationship(writeEvent, SUCCESS);
                    duplicateFlowFileMap.remove(writeEvent.getTargetUri());
                }
            }
        }).onBatchFailure((writeBatch, throwable) -> {
            for(WriteEvent writeEvent : writeBatch.getItems()) {
                routeDocumentToRelationship(writeEvent, FAILURE);
                duplicateFlowFileMap.remove(writeEvent.getTargetUri());
            }
        });
        dataMovementManager.startJob(writeBatcher);
    }

    protected FlowFileInfo getFlowFileInfoForWriteEvent(WriteEvent writeEvent) {
        DocumentMetadataHandle metadata = (DocumentMetadataHandle) writeEvent.getMetadata();
        String flowFileUUID = metadata.getMetadataValues().get("flowFileUUID");
        return uriFlowFileMap.get(flowFileUUID);
    }
    
    protected void routeDocumentToRelationship(WriteEvent writeEvent, Relationship relationship) {
        FlowFileInfo flowFile = getFlowFileInfoForWriteEvent(writeEvent);
        if(flowFile != null) {
            synchronized(flowFile.session) {
                flowFile.session.getProvenanceReporter().send(flowFile.flowFile, writeEvent.getTargetUri());
                flowFile.session.transfer(flowFile.flowFile, relationship);
                flowFile.session.commit();
            }
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Routing " + writeEvent.getTargetUri() + " to " + relationship.getName());
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
     * When a FlowFile is received, hand it off to the WriteBatcher so it can be written to MarkLogic.
     * <p>
     * If a FlowFile is not set (possible because of the TriggerWhenEmpty annotation), then yield is called on the
     * ProcessContext so that Nifi doesn't invoke this method repeatedly when nothing is available. Then, a check is
     * made to determine if flushAsync should be called on the WriteBatcher. This ensures that any batch of documents
     * that is smaller than the WriteBatcher's batch size will be flushed immediately and not have to wait for more
     * FlowFiles to arrive to fill out the batch.
     *
     */
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            FlowFile flowFile = session.get();
            if (flowFile == null) {
                if (shouldFlushIfEmpty) {
                    flushWriteBatcherAsync(this.writeBatcher);
                }
                shouldFlushIfEmpty = false;
                context.yield();
            } else {
                shouldFlushIfEmpty = true;
                
                String duplicateHandler = context.getProperty(DUPLICATE_URI_HANDLING).getValue();
                WriteEvent writeEvent = buildWriteEvent(context, session, flowFile);
                
                String currentUrl = writeEvent.getTargetUri();             
                String currentUUID = flowFile.getAttribute(CoreAttributes.UUID.key());
                String previousUUID = duplicateFlowFileMap.get(currentUrl);                
                
                //Looks like the best place to detect duplicates and handle action because we have access to computed url by this point,
                switch(duplicateHandler) {
                case IGNORE :
                    //Just write the event knowing it will fail during batch write process
                    uriFlowFileMap.put(currentUUID, new FlowFileInfo(flowFile, session,writeEvent));
                    addWriteEvent(this.writeBatcher, writeEvent);
                    break;
                case FAIL_URI:
                    //Quick Fail the routeDocumentToRelationship will cleanup entry
                    if(previousUUID != null && previousUUID != currentUUID) {
                        getLogger().debug("Routing to FAIL_URI:" + currentUUID);
                        uriFlowFileMap.put(currentUUID, new FlowFileInfo(flowFile, session,writeEvent));
                        routeDocumentToRelationship(writeEvent,DUPLICATE_URI);                    

                    } else {
                        //Now just add new WriteEvent
                        uriFlowFileMap.put(currentUUID, new FlowFileInfo(flowFile, session,writeEvent));
                        duplicateFlowFileMap.put(currentUrl,currentUUID);
                        addWriteEvent(this.writeBatcher,writeEvent);
                    }
                    break;

                case CLOSE_BATCH :
                    if(previousUUID != null) {
                        getLogger().info("Closing Batch ... Duplicate Detected for uri:" + writeEvent.getTargetUri());
                            this.closeWriteBatcher();
                    } 
                    uriFlowFileMap.put(currentUUID, new FlowFileInfo(flowFile, session,writeEvent));
                    duplicateFlowFileMap.put(currentUrl,currentUUID);
                    addWriteEvent(this.writeBatcher, writeEvent);
                    break;
                }
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Writing URI: " + writeEvent.getTargetUri());
                }
            }
        } catch (final Throwable t) {
            this.handleThrowable(t, session);
        }
    }
    /*
     * Protected so that it can be overridden for unit testing purposes.
     */
    protected void flushWriteBatcherAsync(WriteBatcher writeBatcher) {
        writeBatcher.flushAsync();
    }

    /*
     * Protected so that it can be overridden for unit testing purposes.
     */
    protected void addWriteEvent(WriteBatcher writeBatcher, WriteEvent writeEvent) {
        writeBatcher.add(writeEvent);
    }

    protected WriteEvent buildWriteEvent(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        String uri = flowFile.getAttribute(context.getProperty(URI_ATTRIBUTE_NAME).getValue());
        final String prefix = context.getProperty(URI_PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        if (prefix != null) {
            uri = prefix + uri;
        }
        final String suffix = context.getProperty(URI_SUFFIX).evaluateAttributeExpressions(flowFile).getValue();
        if (suffix != null) {
            uri += suffix;
        }

        DocumentMetadataHandle metadata = buildMetadataHandle(context, flowFile, context.getProperty(COLLECTIONS), context.getProperty(PERMISSIONS));
        final byte[] content = new byte[(int) flowFile.getSize()];
        session.read(flowFile, inputStream -> StreamUtils.fillBuffer(inputStream, content));

        BytesHandle handle = new BytesHandle(content);

        final String format = context.getProperty(FORMAT).getValue();
        if (format != null) {
            handle.withFormat(Format.valueOf(format));
        } else {
            addFormat(uri, handle);
        }

        final String mimetype = context.getProperty(MIMETYPE).getValue();
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
            for (final PropertyDescriptor propertyDesc: metaProperties) {
                metadata.withMetadataValue(propertyDesc.getName().substring(metaPrefix.length() + 1), context.getProperty(propertyDesc).evaluateAttributeExpressions(flowFile).getValue());
            }
        }
        // Set dynamic properties
        String propertyPrefix = "property";
        List<PropertyDescriptor> propertyProperties = propertiesByPrefix.get(propertyPrefix);
        if (propertyProperties != null) {
            for (final PropertyDescriptor propertyDesc: propertiesByPrefix.get(propertyPrefix)) {
                metadata.withProperty(propertyDesc.getName().substring(propertyPrefix.length() + 1), context.getProperty(propertyDesc).evaluateAttributeExpressions(flowFile).getValue());
            }
        }

        return metadata;
    }

    protected void addFormat(String uri, BytesHandle handle) {
        int extensionStartIndex = uri.lastIndexOf(".");
        if(extensionStartIndex > 0) {
            String extension = uri.substring(extensionStartIndex + 1).toLowerCase();
            switch ( extension ) {
                case "xml" :
                    handle.withFormat(Format.XML);
                    break;
                case "json" :
                    handle.withFormat(Format.JSON);
                    break;
                case "txt" :
                    handle.withFormat(Format.TEXT);
                    break;
                default:
                    handle.withFormat(Format.UNKNOWN);
                    break;
            }
        }
    }
    /*
     * Closes the batch likely due to Duplicate URI detected
     * */
    protected void closeWriteBatcher() {
        if(writeBatcher != null) {
            writeBatcher.flushAndWait();
        }
    }
    @OnShutdown
    @OnStopped
    public void completeWriteBatcherJob() {
        if (writeBatcher != null) {
            getLogger().info("Calling flushAndWait on WriteBatcher");
            writeBatcher.flushAndWait();
            writeBatcher.awaitCompletion();
            getLogger().info("Stopping WriteBatcher job");
            dataMovementManager.stopJob(writeBatcher);
        }
        writeBatcher = null;
        dataMovementManager = null;
    }
}
