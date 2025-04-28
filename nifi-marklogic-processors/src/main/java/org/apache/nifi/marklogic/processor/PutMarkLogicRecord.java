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
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.datamovement.impl.WriteEventImpl;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import org.apache.nifi.annotation.behavior.*;
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
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Tags({"MarkLogic", "Put", "Bulk", "Insert"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Breaks down FlowFiles into batches of Records and inserts documents to a MarkLogic server using the " +
    "MarkLogic Data Movement SDK (DMSDK). Requires a MarkLogic user with the 'rest-writer' privilege.")
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
        .description("The incoming FlowFile will be written to this relationship")
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
        list.add(RESTART_FAILED_BATCHER);
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
        if (context.getProperty(RECORD_COERCE_TYPES) != null &&
            context.getProperty(RECORD_COERCE_TYPES).asBoolean() != null) {
            coerceTypes = context.getProperty(RECORD_COERCE_TYPES).asBoolean();
        }
        if (context.getProperty(RECORD_DROP_UNKNOWN_FIELDS) != null &&
            context.getProperty(RECORD_DROP_UNKNOWN_FIELDS).asBoolean() != null) {
            dropUnknownFields = context.getProperty(RECORD_DROP_UNKNOWN_FIELDS).asBoolean();
        }
    }

    @Override
    public final void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            context.yield();
            return;
        }

        final String uriFieldName = context.getProperty(URI_FIELD_NAME).evaluateAttributeExpressions(flowFile).getValue();

        int added = 0;
        boolean error = false;

        try (final InputStream inStream = session.read(flowFile);
             final RecordReader reader = recordReaderFactory.createRecordReader(flowFile, inStream, getLogger())) {

            final RecordSchema schema = recordSetWriterFactory.getSchema(flowFile.getAttributes(), reader.getSchema());
            final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
            Record record;

            // A FlowFile may produce N records, and for each record, a WriteEvent is created. But the uriToFlowFileMap
            // only needs one entry in it for the original FlowFile; that allows for the batch_success relationship
            // to receive the correct data. It doesn't matter which WriteEvent is added to the map, we just need one
            // and only one entry added to the map.
            boolean addedToFlowFileMap = false;

            while ((record = reader.nextRecord(coerceTypes, dropUnknownFields)) != null) {
                baos.reset();
                try (final RecordSetWriter writer = recordSetWriterFactory.createWriter(getLogger(), schema, baos, flowFile)) {
                    writer.write(record);
                    writer.flush();
                    BytesHandle bytesHandle = new BytesHandle().with(baos.toByteArray());
                    final String uri = uriFieldName == null ? UUID.randomUUID().toString() : record.getAsString(uriFieldName);
                    WriteEvent writeEvent = buildWriteEvent(context, flowFile, uri, bytesHandle);
                    if (!addedToFlowFileMap) {
                        uriFlowFileMap.put(flowFile.getAttribute(CoreAttributes.UUID.key()), new FlowFileInfo(flowFile, session, writeEvent));
                        addedToFlowFileMap = true;
                    }
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
                flushAndWaitWithoutFailing(writeBatcher);
                session.getProvenanceReporter().send(flowFile, url, String.format("Added %d documents to MarkLogic.", added));
                session.transfer(flowFile, ORIGINAL);
                uriFlowFileMap.remove(flowFile.getAttribute(CoreAttributes.UUID.key()));
                getLogger().info("Inserted {} records into MarkLogic", added);
            }
        }
        session.commitAsync();
    }

    @Override
    protected void transferFlowFile(WriteEvent writeEvent, Relationship relationship) {
        FlowFileInfo flowFileInfo = getFlowFileInfoForWriteEvent(writeEvent);
        if (flowFileInfo != null) {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Routing " + writeEvent.getTargetUri() + " to " + relationship.getName());
            }
            synchronized (flowFileInfo.session) {
                FlowFile flowFile = flowFileInfo.session.create();
                flowFileInfo.session.getProvenanceReporter().send(flowFile, writeEvent.getTargetUri());
                flowFileInfo.session.transfer(flowFile, relationship);
            }
        }
    }

    private WriteEvent buildWriteEvent(
        final ProcessContext context,
        final FlowFile flowFile,
        String uri,
        final BytesHandle contentHandle
    ) {
        final String prefix = context.getProperty(URI_PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        if (prefix != null) {
            uri = prefix + uri;
        }
        final String suffix = context.getProperty(URI_SUFFIX).evaluateAttributeExpressions(flowFile).getValue();
        if (suffix != null) {
            uri += suffix;
        }
        uri = uri.replaceAll("//", "/");

        DocumentMetadataHandle metadata = buildMetadataHandle(context, flowFile, context.getProperty(COLLECTIONS), context.getProperty(PERMISSIONS));
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

        return new WriteEventImpl()
            .withTargetUri(uri)
            .withMetadata(metadata)
            .withContent(contentHandle);
    }

}
