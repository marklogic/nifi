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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.datamovement.impl.WriteEventImpl;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;

@EventDriven
@Tags({"MarkLogic", "Put", "Bulk", "Insert"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Breaks down FlowFiles into batches of Records and inserts JSON documents to a MarkLogic server using the " +
        "MarkLogic Data Movement SDK (DMSDK)")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@DynamicProperty(name = "trans: Server transform parameter name, property: Property name to add, meta: Metadata name to add",
    value = "trans: Value of the server transform parameter, property: Property value to add, meta: Metadata value to add",
    description = "Depending on the property prefix, routes data to transform, metadata, or property.",
    expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
public class PutMarkLogicRecord extends PutMarkLogic {
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for incoming FlowFiles")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to serialize the data before sending to MarkLogic")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    public static final PropertyDescriptor URI_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("URI Field Name")
            .displayName("URI Field Name")
            .required(false)
            .description("Field name used for generating the document URI. If none is specified, a UUID is generated.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor RECORD_COERCE_TYPES = new PropertyDescriptor.Builder()
        .name("Coerce Types in Records")
        .displayName("Coerce Types in Records")
        .required(true)
        .description("Whether or not fields in the Record should be validated against the schema and coerced when necessary")
        .addValidator(Validator.VALID)
        .allowableValues("true", "false")
        .defaultValue("true")
        .build();

    public static final PropertyDescriptor RECORD_DROP_UNKNOWN_FIELDS = new PropertyDescriptor.Builder()
        .name("Drop Unknown Fields in Records")
        .displayName("Drop Unknown Fields in Records")
        .required(true)
        .description("If true, any field that is found in the data that is not present in the schema will be dropped. " +
	        "If false, those fields will still be part of the Record (though their type cannot be coerced, since the schema does not provide a type for it).")
        .addValidator(Validator.VALID)
        .allowableValues("true", "false")
        .defaultValue("true")
        .build();

    protected static final Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original FlowFiles coming into PutMarkLogicRecord.")
            .build();

    private RecordReaderFactory recordReaderFactory;
    private RecordSetWriterFactory recordSetWriterFactory;

    @Override
    public void init(ProcessorInitializationContext context) {
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(DATABASE_CLIENT_SERVICE);
        list.add(BATCH_SIZE);
        list.add(THREAD_COUNT);
        list.add(RECORD_READER);
        list.add(RECORD_WRITER);
        list.add(RECORD_COERCE_TYPES);
        list.add(RECORD_DROP_UNKNOWN_FIELDS);
        list.add(COLLECTIONS);
        list.add(FORMAT);
        list.add(JOB_ID);
        list.add(JOB_NAME);
        list.add(MIMETYPE);
        list.add(PERMISSIONS);
        list.add(TRANSFORM);
        list.add(TEMPORAL_COLLECTION);
        list.add(URI_FIELD_NAME);
        list.add(URI_PREFIX);
        list.add(URI_SUFFIX);
        properties = Collections.unmodifiableList(list);
        Set<Relationship> set = new HashSet<>();
        set.add(BATCH_SUCCESS);
        set.add(SUCCESS);
        set.add(ORIGINAL);
        set.add(FAILURE);
        relationships = Collections.unmodifiableSet(set);
    }

    private boolean coerceTypes;
    private boolean dropUnknownFields;

    @OnScheduled
    public void initializeFactories(ProcessContext context) {
        recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        recordSetWriterFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        coerceTypes = context.getProperty(RECORD_COERCE_TYPES).asBoolean();
        dropUnknownFields = context.getProperty(RECORD_DROP_UNKNOWN_FIELDS).asBoolean();
    }

	@Override
    public final void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            context.yield();
            return;
        }

        final String uriFieldName = context.getProperty(URI_FIELD_NAME).evaluateAttributeExpressions(flowFile).getValue();

        int added   = 0;
        boolean error = false;

        try (final InputStream inStream = session.read(flowFile);
            final RecordReader reader = recordReaderFactory.createRecordReader(flowFile, inStream, getLogger())) {

            final RecordSchema schema = recordSetWriterFactory.getSchema(flowFile.getAttributes(), reader.getSchema());
            final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
            Record record;
            while ((record = reader.nextRecord(coerceTypes, dropUnknownFields)) != null) {
                baos.reset();
                Map<String, String> additionalAttributes;
                try (final RecordSetWriter writer = recordSetWriterFactory.createWriter(getLogger(), schema, baos)) {
                    final WriteResult writeResult = writer.write(record);
                    additionalAttributes = writeResult.getAttributes();
                    writer.flush();
                    BytesHandle bytesHandle = new BytesHandle().with(baos.toByteArray());
                    final String uriKey = uriFieldName == null ? UUID.randomUUID().toString() : record.getAsString(uriFieldName);
                    WriteEvent writeEvent = buildWriteEvent(context, session, flowFile, uriKey, bytesHandle, additionalAttributes);
                    uriFlowFileMap.put(uriKey, new FlowFileInfo(flowFile, session,writeEvent));
                    this.addWriteEvent(writeBatcher, writeEvent);
                    
                    added++;
                }
            }
        } catch (SchemaNotFoundException | IOException | MalformedRecordException e) {
            getLogger().error("PutMarkLogicRecord failed with error:", e);
            session.transfer(flowFile, FAILURE);
            context.yield();
            error = true;
        } finally {
            if (!error) {
                DatabaseClient client = getDatabaseClient(context);
                String url = client != null
                        ? client.getHost() + ":" + client.getPort()
                        : "MarkLogic cluster";
                writeBatcher.flushAndWait();
                session.getProvenanceReporter().send(flowFile, url, String.format("Added %d documents to MarkLogic.", added));
                session.transfer(flowFile, ORIGINAL);
                uriFlowFileMap.remove(flowFile.getAttribute(CoreAttributes.UUID.key()));
                getLogger().info("Inserted {} records into MarkLogic", new Object[]{ added });
            }
        }
        session.commit();
    }

    @Override
    protected void routeDocumentToRelationship(WriteEvent writeEvent, Relationship relationship) {
        FlowFileInfo flowFileInfo = getFlowFileInfoForWriteEvent(writeEvent);
        if(flowFileInfo != null) {
            synchronized(flowFileInfo.session) {
                FlowFile flowFile = flowFileInfo.session.create();
                flowFileInfo.session.getProvenanceReporter().send(flowFile, writeEvent.getTargetUri());
                flowFileInfo.session.transfer(flowFile, relationship);
            }
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Routing " + writeEvent.getTargetUri() + " to " + relationship.getName());
            }
        }
    }

    protected WriteEvent buildWriteEvent(
            final ProcessContext context,
            final ProcessSession session,
            final FlowFile flowFile,
            String uri,
            final BytesHandle contentHandle,
            final Map<String, String> additionalAttributes
    ) {
        final String prefix = context.getProperty(URI_PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        if (prefix != null) {
            uri = prefix + uri;
        }
        final String suffix = context.getProperty(URI_SUFFIX).evaluateAttributeExpressions(flowFile).getValue();
        if (suffix != null) {
            uri += suffix;
        }
        uri.replaceAll("//", "/");

        DocumentMetadataHandle metadata = buildMetadataHandle(context, flowFile, context.getProperty(COLLECTIONS), context.getProperty(PERMISSIONS));
        // Add the flow file UUID for Provenance purposes and for sending them
        // to the appropriate relationship
        String flowFileUUID = flowFile.getAttribute(CoreAttributes.UUID.key());
        final String format = context.getProperty(FORMAT).getValue();
        if (format != null) {
            contentHandle.withFormat(Format.valueOf(format));
        } else {
            addFormat(uri, contentHandle);
        }

        final String mimetype = context.getProperty(MIMETYPE).getValue();
        if (mimetype != null) {
            contentHandle.withMimetype(mimetype);
        }

        //uriFlowFileMap.put(flowFileUUID, new FlowFileInfo(flowFile, session,writeEvent));
        return new WriteEventImpl()
            .withTargetUri(uri)
            .withMetadata(metadata)
            .withContent(contentHandle);
    }

}