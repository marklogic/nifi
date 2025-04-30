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
import com.marklogic.client.datamovement.*;
import com.marklogic.client.datamovement.impl.JobReportImpl;
import com.marklogic.client.document.DocumentManager.Metadata;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.*;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryBuilder.Operator;
import com.marklogic.client.query.ValuesDefinition;
import com.marklogic.client.util.EditableNamespaceContext;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.*;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.marklogic.processor.util.QueryBatcherBuilder;
import org.apache.nifi.marklogic.processor.util.QueryBatcherContext;
import org.apache.nifi.marklogic.processor.util.QueryTypes;
import org.apache.nifi.marklogic.processor.util.RangeIndexQuery;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Supports querying MarkLogic via the Data Movement SDK (DMSDK). More information on DMSDK can be found at
 * <a href="https://docs.marklogic.com/guide/java/data-movement#id_46947">...</a> .
 * <p>
 * The general approach is to require the user to specify a type of query and then a textual representation of a query
 * associated with that type.
 */
@Tags({"MarkLogic", "Get", "Query", "Read"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@CapabilityDescription("Creates FlowFiles from batches of documents, matching the given criteria,"
    + " retrieved from a MarkLogic server using the MarkLogic Data Movement SDK (DMSDK). Requires a MarkLogic user with " +
    "the 'rest-reader' privilege.")
@DynamicProperties({
    @DynamicProperty(
        name = "ns:{prefix}",
        value = "A namespace URI",
        description = "Define namespace prefixes and URIs that can be used to construct State Index values when " +
            "State Index type is either ELEMENT or PATH",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    ),
    @DynamicProperty(
        name = "trans:{name}",
        value = "The value of a parameter to be passed to a REST server transform",
        description = "A transform parameter with name equal to that of '{name}' will be passed to the REST server " +
            "transform identified by the optional 'Server Transform' property",
        expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT
    )
})
@WritesAttributes({
    @WritesAttribute(
        attribute = "filename",
        description = "The filename is set to the URI of the document retrieved from MarkLogic"
    )
})
@Stateful(description = "Can keep state of a range index value to restrict future queries", scopes = {Scope.CLUSTER})
public class QueryMarkLogic extends AbstractMarkLogicProcessor {

    public static final PropertyDescriptor CONSISTENT_SNAPSHOT = new PropertyDescriptor.Builder()
        .name("Consistent Snapshot").displayName("Consistent Snapshot").defaultValue("true")
        .description("Set to 'true' for query results to be based on the same server timestamp")
        .required(true).addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor.Builder().name("Return Type")
        .displayName("Return Type").defaultValue(ReturnTypes.DOCUMENTS.getValue())
        .description("The type of data to return from the query").required(true)
        .allowableValues(ReturnTypes.allValues).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor INCLUDE_DOCUMENT_PROPERTIES = new PropertyDescriptor.Builder()
        .allowableValues("true", "false")
        .name("Include Document Properties").displayName("Include Document Properties").defaultValue("true")
        .description("When the 'Return Type' selection results in metadata being included for each document, this can " +
            "be set to 'false' to prevent document properties from being included. This may be desirable for when " +
            "documents have large properties fragments that are not worth including in outgoing FlowFiles.")
        .required(false).addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder().name("Query").displayName("Query")
        .description("Query text that corresponds with the selected Query Type").required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor QUERY_TYPE = new PropertyDescriptor.Builder().name("Query Type")
        .displayName("Query Type").description("Type of query that will be used to retrieve data from MarkLogic. " +
            "For more information on combined queries, see https://docs.marklogic.com/guide/java/searches#id_76144 . " +
            "For more information on structured queries, see https://docs.marklogic.com/guide/java/searches#id_70572 . " +
            "For more information on string queries, see https://docs.marklogic.com/guide/java/searches#id_80640 . ")
        .required(true).allowableValues(QueryTypes.allValues).defaultValue(QueryTypes.COMBINED_JSON.getValue())
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor STATE_INDEX = new PropertyDescriptor.Builder().name("State Index")
        .displayName("State Index")
        .description("Definition of the index which will be used to keep state to restrict future calls; only supports " +
            "xs:dateTime indices; see State Index Type for examples of what to provide here")
        .required(false).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(Validator.VALID).build();

    public static final PropertyDescriptor STATE_INDEX_TYPE = new PropertyDescriptor.Builder().name("State Index Type")
        .displayName("State Index Type").description("Type of index to determine state for next set of documents")
        .required(true).expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues(IndexTypes.allValues).defaultValue(IndexTypes.JSON_PROPERTY.getValue())
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor COLLECTIONS = new PropertyDescriptor.Builder().name("Collections")
        .displayName("Collections")
        .description(
            "Deprecated; use Query Type and Query instead. Comma-separated list of collections to query.")
        .required(false).addValidator(Validator.VALID).build();

    protected static final Relationship SUCCESS = new Relationship.Builder().name("success")
        .description("For each document matching the query, a FlowFile is sent to this relationship").build();

    protected static final Relationship FAILURE = new Relationship.Builder().name("failure")
        .description("If an error occurs while retrieving a batch of records for a query, a FlowFile will be sent to this relationship").build();

    protected static final Relationship ORIGINAL = new Relationship.Builder().name("original")
        // In 1.16.3.2, this is set to true so ApplyTransformMarkLogic can include it without breaking existing
        // instances of that processor
        .autoTerminateDefault(true)
        .description("Receives the incoming FlowFile, or a new FlowFile if no incoming one exists").build();

    // Keeps track of the server timestamp at the point in time in which the query was issued. Only applies for when the
    // user has configured the inputs for keeping track of the latest dateTime so that it can be used the next time
    // the processor runs.
    private volatile AtomicLong serverTimestamp = new AtomicLong(0);

    // This is only captured here to facilitate unit-testing
    private QueryBatcher queryBatcher;

    protected QueryBatcher getQueryBatcherForTesting() {
        return this.queryBatcher;
    }

    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);

        List<PropertyDescriptor> list = new ArrayList<>(properties);
        list.add(CONSISTENT_SNAPSHOT);
        list.add(QUERY);
        list.add(QUERY_TYPE);
        list.add(RETURN_TYPE);
        list.add(INCLUDE_DOCUMENT_PROPERTIES);
        list.add(TRANSFORM);
        list.add(STATE_INDEX);
        list.add(STATE_INDEX_TYPE);
        list.add(COLLECTIONS);
        properties = Collections.unmodifiableList(list);

        Set<Relationship> set = new HashSet<>();
        set.add(SUCCESS);
        set.add(FAILURE);
        set.add(ORIGINAL);
        relationships = Collections.unmodifiableSet(set);
    }

    /**
     * Overrides the parent class method to provide a warning about the deprecated "Collections" property.
     *
     * @param validationContext provides a mechanism for obtaining externally
     *                          managed values, such as property values and supplies convenience methods
     *                          for operating on those values
     * @return
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        Set<ValidationResult> validationResultSet = new HashSet<>();
        Objects.requireNonNull(validationContext.getProperty(COLLECTIONS), "COLLECTIONS property should not be null");
        String collections = validationContext.getProperty(COLLECTIONS).getValue();
        Objects.requireNonNull(validationContext.getProperty(QUERY), "QUERY property should not be null");
        String query = validationContext.getProperty(QUERY).getValue();
        if (collections == null && query == null) {
            validationResultSet.add(new ValidationResult.Builder().subject("Query").valid(false)
                .explanation("The Query value must be set. "
                    + "The deprecated Collections property will be migrated appropriately.")
                .build());
        }
        return validationResultSet;
    }

    /**
     * Constructs a DMSDK QueryBatcher based on the inputs provided by the user, and then runs and waits for the
     * QueryBatcher to process the URIs matching the user's inputs.
     *
     * @param context        provides access to convenience methods for obtaining
     *                       property values, delaying the scheduling of the processor, provides
     *                       access to Controller Services, etc.
     * @param sessionFactory provides access to a {@link ProcessSession}, which
     *                       can be used for accessing FlowFiles, etc.
     * @throws ProcessException
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        super.populatePropertiesByPrefix(context);

        FlowFile incomingFlowFile = session.get();
        if (incomingFlowFile == null) {
            incomingFlowFile = session.create();
        }

        try {
            QueryBatcherContext queryBatcherContext = newQueryBatcher(context, incomingFlowFile);
            session.putAttribute(incomingFlowFile, "marklogic-query", queryBatcherContext.getQueryRepresentation());
            configureQueryBatcher(context, session, incomingFlowFile, queryBatcherContext.getBatcher());

            // Save a reference to this solely to facilitate unit testing
            this.queryBatcher = queryBatcherContext.getBatcher();

            // Transfer the incoming FF immediately in case the QueryBatcher returns no results
            transferAndCommit(session, incomingFlowFile, ORIGINAL);
            runQueryBatcherJob(queryBatcherContext);
        } catch (Throwable t) {
            logErrorAndTransfer(t, incomingFlowFile, session, FAILURE);
            context.yield();
        }
    }

    /**
     * Constructs a new QueryBatcher based on inputs provided by the user.
     *
     * @param context
     * @param incomingFlowFile
     * @return A Tuple is returned to simplify the interface here so that this method can create and return both the
     * DataMovementManager and QueryBatcher. Both objects are needed to run the QueryBatcher.
     */
    private QueryBatcherContext newQueryBatcher(ProcessContext context, FlowFile incomingFlowFile) {
        DatabaseClient client = getDatabaseClient(context);
        QueryBatcherBuilder.QueryTypeAndValue queryTypeAndValue = determineQueryTypeAndValue(context, incomingFlowFile);
        RangeIndexQuery stateRangeIndexQuery = buildStateQuery(client, context, incomingFlowFile);
        return new QueryBatcherBuilder(client).newQueryBatcher(queryTypeAndValue, stateRangeIndexQuery);
    }

    /**
     * @param context
     * @param incomingFlowFile
     * @return a {@code QueryTypeAndValue} based on inputs provided by the user
     */
    private QueryBatcherBuilder.QueryTypeAndValue determineQueryTypeAndValue(ProcessContext context, FlowFile incomingFlowFile) {
        // Migrate deprecated "Collections" property to the preferred "Collection" query type
        Objects.requireNonNull(context.getProperty(COLLECTIONS), "COLLECTIONS property should not be null");
        final String collectionsValue = context.getProperty(COLLECTIONS).getValue();
        if (StringUtils.isNotBlank(collectionsValue)) {
            return new QueryBatcherBuilder.QueryTypeAndValue(QueryTypes.COLLECTION.getValue(), collectionsValue);
        }
        Objects.requireNonNull(context.getProperty(QUERY_TYPE), "QUERY_TYPE property should not be null");
        Objects.requireNonNull(context.getProperty(QUERY), "QUERY property should not be null");
        return new QueryBatcherBuilder.QueryTypeAndValue(
            context.getProperty(QUERY_TYPE).getValue(),
            context.getProperty(QUERY).evaluateAttributeExpressions(incomingFlowFile).getValue()
        );
    }

    /**
     * Configure a newly-constructed QueryBatcher.
     *
     * @param context
     * @param session
     * @param incomingFlowFile
     * @param queryBatcher
     */
    private void configureQueryBatcher(ProcessContext context, ProcessSession session, FlowFile incomingFlowFile, QueryBatcher queryBatcher) {
        Objects.requireNonNull(context.getProperty(BATCH_SIZE), "BATCH_SIZE property should not be null");
        if (context.getProperty(BATCH_SIZE).asInteger() != null) {
            queryBatcher.withBatchSize(context.getProperty(BATCH_SIZE).asInteger());
        }
        Objects.requireNonNull(context.getProperty(THREAD_COUNT), "THREAD_COUNT property should not be null");
        if (context.getProperty(THREAD_COUNT).asInteger() != null) {
            queryBatcher.withThreadCount(context.getProperty(THREAD_COUNT).asInteger());
        }

        Map<String, String> attributesToCopy = getAttributesToCopy(incomingFlowFile);
        QueryBatchListener batchListener = buildQueryBatchListener(context, session, attributesToCopy);
        queryBatcher.onUrisReady(batchListener);

        queryBatcher.onUrisReady(batch -> {
            if (batch.getJobBatchNumber() == 1) {
                serverTimestamp.set(batch.getServerTimestamp());
            }
        });

        configureJobCompletionListener(context, incomingFlowFile, queryBatcher);

        /**
         * onQueryFailure is confusing in that - based on an analysis of the QueryBatcherImpl source code - the listener
         * likely never be invoked, and certainly not on account of a query failure. This is here only in the
         * extremely unlikely event that a listener is invoked.
         */
        queryBatcher.onQueryFailure(ex ->
            logErrorAndTransfer(ex, createFlowFileWithAttributes(session, attributesToCopy), session, FAILURE));
    }

    /**
     * Protected so that subclasses can override it.
     *
     * @param context
     * @param session
     * @return
     */
    protected QueryBatchListener buildQueryBatchListener(ProcessContext context, ProcessSession session, Map<String, String> incomingAttributes) {
        Objects.requireNonNull(context.getProperty(RETURN_TYPE), "RETURN_TYPE property should not be null");
        final boolean retrieveFullDocument =
            ReturnTypes.DOCUMENTS_STR.equals(context.getProperty(RETURN_TYPE).getValue()) ||
                ReturnTypes.DOCUMENTS_AND_META_STR.equals(context.getProperty(RETURN_TYPE).getValue());

        return retrieveFullDocument ?
            buildFullDocumentExporter(context, session, incomingAttributes) :
            buildNoDocumentExporter(context, session, incomingAttributes);
    }

    /**
     * Used to trigger a commitAsync after each batch is processed.
     */
    static class DocumentExporter extends ExportListener {
        private ProcessSession session;
        public DocumentExporter(ProcessSession session) {
            this.session = session;
        }
        @Override
        public void processEvent(QueryBatch batch) {
            super.processEvent(batch);
            // Ensures that the session is committed after all docs in a batch have been exported. Note that in the
            // event of a batch failure, this will be called twice, which does not seem to have any negative effects.
            synchronized (session) {
                this.session.commitAsync();
            }
        }
    }

    /**
     * Used for when the users asks for documents and possibly metadata as well.
     *
     * @param context
     * @param session
     * @return
     */
    private ExportListener buildFullDocumentExporter(ProcessContext context, ProcessSession session, Map<String, String> incomingAttributes) {
        final boolean retrieveMetadata = shouldRetrieveMetadata(context);

        ExportListener exportListener = new DocumentExporter(session).onDocumentReady(doc -> {
            synchronized (session) {
                final FlowFile flowFile = session.write(
                    createFlowFileWithAttributes(session, incomingAttributes),
                    out -> out.write(doc.getContent(new BytesHandle()).get())
                );
                session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), doc.getUri());
                if (retrieveMetadata) {
                    addDocumentMetadata(context, session, flowFile, doc.getMetadata(new DocumentMetadataHandle()));
                }
                session.transfer(flowFile, SUCCESS);
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Routing " + doc.getUri() + " to " + SUCCESS.getName());
                }
            }
        }).onFailure((batch, throwable) -> {
            getLogger().error("Unable to export batch of URIs; cause: " + throwable.getMessage());
            transferBatch(session, incomingAttributes, batch, FAILURE, throwable);
        });

        if (retrieveMetadata) {
            exportListener.withMetadataCategory(Metadata.ALL);
        }
        Objects.requireNonNull(context.getProperty(CONSISTENT_SNAPSHOT), "CONSISTENT_SNAPSHOT property should not be null");
        if (Boolean.TRUE.equals(context.getProperty(CONSISTENT_SNAPSHOT).asBoolean())) {
            exportListener.withConsistentSnapshot();
        }
        ServerTransform transform = this.buildServerTransform(context);
        if (transform != null) {
            exportListener.withTransform(transform);
        }
        return exportListener;
    }

    /**
     * Convenience method for transferring a batch to a relationship, with an optional Throwable supported.
     */
    protected final void transferBatch(ProcessSession session, Map<String, String> incomingAttributes, QueryBatch batch, Relationship relationship, Throwable throwable) {
        synchronized (session) {
            for (String uri : batch.getItems()) {
                FlowFile flowFile = createFlowFileWithAttributes(session, incomingAttributes);
                session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), uri);
                if (throwable != null) {
                    addErrorMessageToFlowFile(throwable.getMessage(), flowFile, session);
                }
                session.transfer(flowFile, relationship);
            }
            session.commitAsync();
        }
    }

    /**
     * Used for when the user asks for URIs only or just URIs and document metadata.
     *
     * @param context
     * @param session
     * @return
     */
    private QueryBatchListener buildNoDocumentExporter(ProcessContext context, ProcessSession session, Map<String, String> incomingAttributes) {
        Objects.requireNonNull(context.getProperty(CONSISTENT_SNAPSHOT), "CONSISTENT_SNAPSHOT property should not be null");
        final boolean consistentSnapshot = Boolean.TRUE.equals(context.getProperty(CONSISTENT_SNAPSHOT).asBoolean());
        return batch -> {
            synchronized (session) {
                Arrays.stream(batch.getItems()).forEach(uri -> {
                    try {
                        FlowFile flowFile = createFlowFileWithAttributes(session, incomingAttributes);
                        session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), uri);
                        if (shouldRetrieveMetadata(context)) {
                            DocumentMetadataHandle metadata = new DocumentMetadataHandle();
                            if (consistentSnapshot) {
                                metadata.setServerTimestamp(batch.getServerTimestamp());
                            }
                            batch.getClient().newDocumentManager().readMetadata(uri, metadata);
                            addDocumentMetadata(context, session, flowFile, metadata);
                        }
                        session.transfer(flowFile, SUCCESS);
                        if (getLogger().isDebugEnabled()) {
                            getLogger().debug("Routing " + uri + " to " + SUCCESS.getName());
                        }
                    } catch (Throwable throwable) {
                        // Does not yet have test coverage as there's not yet a way of forcing an error in the code
                        // within the "try" block.
                        FlowFile flowFile = createFlowFileWithAttributes(session, incomingAttributes);
                        session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), uri);
                        addErrorMessageToFlowFile(throwable.getMessage(), flowFile, session);
                        session.transfer(flowFile, FAILURE);
                    }
                });
                session.commitAsync();
            }
        };
    }

    private boolean shouldRetrieveMetadata(ProcessContext context) {
        Objects.requireNonNull(context.getProperty(RETURN_TYPE), "RETURN_TYPE property should not be null");
        String returnType = context.getProperty(RETURN_TYPE).getValue();
        return ReturnTypes.META.getValue().equals(returnType) || ReturnTypes.DOCUMENTS_AND_META.getValue().equals(returnType);
    }

    private void addDocumentMetadata(ProcessContext context, ProcessSession session, FlowFile flowFile, DocumentMetadataHandle metadata) {
        // For attributes added in 1.16.3.1, we're using a "marklogic-" prefix to avoid collisions with attributes
        // added by other processors.
        session.putAttribute(flowFile, "marklogic-collections", String.join(",", metadata.getCollections()));

        session.putAttribute(flowFile, "marklogic-quality", metadata.getQuality() + "");

        List<String> permissions = new ArrayList<>();
        DocumentMetadataHandle.DocumentPermissions docPerms = metadata.getPermissions();
        for (String role : docPerms.keySet()) {
            for (DocumentMetadataHandle.Capability capability : docPerms.get(role)) {
                permissions.add(role);
                // Lowercase is used to mirror how MLCP expects permissions to be defined
                permissions.add(capability.name().toLowerCase());
            }
        }
        session.putAttribute(flowFile, "marklogic-permissions", String.join(",", permissions));

        metadata.getMetadataValues().forEach((metaKey, metaValue) -> {
            session.putAttribute(flowFile, "meta:" + metaKey, metaValue);
        });

        Objects.requireNonNull(context.getProperty(INCLUDE_DOCUMENT_PROPERTIES), "INCLUDE_DOCUMENT_PROPERTIES property should not be null");
        final boolean includeProperties = Boolean.TRUE.equals(context.getProperty(INCLUDE_DOCUMENT_PROPERTIES).asBoolean());
        if (includeProperties) {
            metadata.getProperties().forEach((qname, propertyValue) -> {
                session.putAttribute(flowFile, "property:" + qname.toString(), propertyValue.toString());
            });
        }
    }

    /**
     * Runs the {@code QueryBatcher} job and waits for it to complete.
     *
     * @param queryBatcherContext
     */
    private void runQueryBatcherJob(QueryBatcherContext queryBatcherContext) {
        getLogger().info("Starting job");
        queryBatcherContext.getManager().startJob(queryBatcherContext.getBatcher());
        getLogger().info("Awaiting job completion");
        queryBatcherContext.getBatcher().awaitCompletion();
        getLogger().info("Stopping job");
        queryBatcherContext.getManager().stopJob(queryBatcherContext.getBatcher());
    }

    /**
     * Returns a RangeIndexQuery if the user has defined the STATE_INDEX property and a dateTime is found in the NiFi
     * state map from a previous run of this processor. This query can then be combined with the query based on the
     * user's inputs to only select URIs with a dateTime value greater than the value found in the NiFi state map.
     * This effectively solves the problem of "Only process URIs newer than when the processor last ran".
     *
     * @param client
     * @param context
     * @param incomingFlowFile
     * @return
     */
    private RangeIndexQuery buildStateQuery(DatabaseClient client, ProcessContext context, FlowFile incomingFlowFile) {
        Objects.requireNonNull(context.getProperty(STATE_INDEX), "STATE_INDEX property should not be null");
        if (!context.getProperty(STATE_INDEX).isSet()) {
            return null;
        }

        String previousQueryDateTime = getPreviousDateTimeFromStateMap(context);
        if (StringUtils.isEmpty(previousQueryDateTime)) {
            return null;
        }

        String stateIndexValue = context.getProperty(STATE_INDEX).evaluateAttributeExpressions(incomingFlowFile).getValue();
        String stateIndexTypeValue = context.getProperty(STATE_INDEX_TYPE).getValue();

        EditableNamespaceContext namespaces = buildNamespacesForRangeIndexQuery(context, incomingFlowFile);
        StructuredQueryBuilder queryBuilder = client.newQueryManager().newStructuredQueryBuilder();
        queryBuilder.setNamespaces(namespaces);

        return new RangeIndexQuery(queryBuilder, stateIndexTypeValue, stateIndexValue, "xs:dateTime", Operator.GT,
            previousQueryDateTime);
    }

    /**
     * The NiFi state map is used to keep track of values across processor runs. For this processor, the state map is
     * used to keep track of the most recent dateTime value in the index specified by the user that was captured during
     * the previous run of this processor.
     *
     * @param context
     * @return
     */
    private String getPreviousDateTimeFromStateMap(ProcessContext context) {
        StateMap stateMap;
        try {
            stateMap = context.getStateManager().getState(Scope.CLUSTER);
        } catch (IOException e) {
            getLogger().error("Unable to build range index query for previous dateTime; failed to get state map: " + e.getMessage(), e);
            return null;
        }

        // "queryState" is not very descriptive, but it has to be preserved for backwards compatibility
        return stateMap.get("queryState");
    }

    /**
     * Returns a set of namespaces based on user-defined properties that begin with "ns". The namespaces can then be
     * referenced by the range index query on the dateTime of the state index.
     *
     * @param context
     * @param incomingFlowFile
     * @return
     */
    private EditableNamespaceContext buildNamespacesForRangeIndexQuery(ProcessContext context, FlowFile incomingFlowFile) {
        List<PropertyDescriptor> namespaceProperties = propertiesByPrefix.get("ns");
        EditableNamespaceContext namespaces = new EditableNamespaceContext();
        if (namespaceProperties != null) {
            for (PropertyDescriptor propertyDesc : namespaceProperties) {
                Objects.requireNonNull(context.getProperty(propertyDesc), "The " + propertyDesc.getName() + " context property should not be null");
                namespaces.put(propertyDesc.getName().substring(3),
                    context.getProperty(propertyDesc).evaluateAttributeExpressions(incomingFlowFile).getValue());
            }
        }
        return namespaces;
    }

    /**
     * After the QueryBatcher job completes, gets the most recent value from the dateTime index specified by the user
     * and stores it in the NiFi state map. This allows it to be used the next time this processor runs in a range
     * index query to ensure that only URIs with a dateTime greater than the stored value are returned.
     *
     * @param context
     * @param incomingFlowFile
     * @param queryBatcher
     */
    private void configureJobCompletionListener(ProcessContext context, FlowFile incomingFlowFile, QueryBatcher queryBatcher) {
        queryBatcher.onJobCompletion(batcher -> {
            JobReport report = new JobReportImpl(batcher);
            if (report.getSuccessEventsCount() == 0) {
                context.yield();
            }
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("ML Query Job Complete [Success Count=" + report.getSuccessEventsCount()
                    + "] [Failure Count=" + report.getFailureEventsCount() + "]");
            }

            Objects.requireNonNull(context.getProperty(STATE_INDEX), "STATE_INDEX property should not be null");
            boolean stateValueShouldBeUpdated = report.getFailureBatchesCount() == 0 &&
                context.getProperty(STATE_INDEX).isSet();

            if (stateValueShouldBeUpdated) {
                Objects.requireNonNull(batcher.getPrimaryClient(), "batcher.getPrimaryClient variable should not be null");
                QueryManager queryMgr = batcher.getPrimaryClient().newQueryManager();
                ValuesDefinition valuesDef = queryMgr.newValuesDefinition("state");
                RawCombinedQueryDefinition qDef = queryMgr.newRawCombinedQueryDefinition(
                    new StringHandle(buildStateConstraintOptions(context, incomingFlowFile)).withFormat(Format.JSON));
                valuesDef.setQueryDefinition(qDef);
                valuesDef.setAggregate("max");
                ValuesHandle valuesResult = new ValuesHandle();
                valuesResult.setPointInTimeQueryTimestamp(serverTimestamp.get());
                valuesResult.setQueryCriteria(valuesDef);
                valuesResult = queryMgr.values(valuesDef, valuesResult);
                String queryStateValue = valuesResult.getAggregate("max").getValue();
                try {
                    StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
                    Map<String, String> alterMap = new HashMap<>(stateMap.toMap());
                    alterMap.put("queryState", queryStateValue);
                    context.getStateManager().setState(alterMap, Scope.CLUSTER);
                } catch (IOException e) {
                    getLogger().error("{} Failed to store state", new Object[]{this});
                }
            }
        });
    }

    /**
     * Builds a set of search options for getting the most recent value from the index identified by the user.
     *
     * @param context
     * @param flowFile
     * @return
     */
    private String buildStateConstraintOptions(final ProcessContext context, final FlowFile flowFile) {
        JsonObject rootObject = new JsonObject();
        JsonObject searchObject = new JsonObject();
        rootObject.add("search", searchObject);
        JsonObject optionsObject = new JsonObject();
        searchObject.add("options", optionsObject);
        JsonArray valuesArray = new JsonArray();
        optionsObject.add("values", valuesArray);
        JsonObject constraintObject = new JsonObject();
        valuesArray.add(constraintObject);
        constraintObject.addProperty("name", "state");
        JsonObject rangeObject = new JsonObject();
        constraintObject.add("range", rangeObject);
        rangeObject.addProperty("type", "xs:dateTime");
        Objects.requireNonNull(context.getProperty(STATE_INDEX_TYPE), "STATE_INDEX_TYPE property should not be null");
        String stateIndexTypeValue = context.getProperty(STATE_INDEX_TYPE).getValue();
        Objects.requireNonNull(context.getProperty(STATE_INDEX), "STATE_INDEX property should not be null");
        String stateIndexValue = context.getProperty(STATE_INDEX).evaluateAttributeExpressions(flowFile).getValue();
        switch (stateIndexTypeValue) {
            case IndexTypes.ELEMENT_STR:
                JsonObject elementObject = new JsonObject();
                boolean hasNamespace = stateIndexValue.contains(":");
                String[] parts = stateIndexValue.split(":", 2);
                String name = (hasNamespace) ? parts[1] : stateIndexValue;
                String ns = (hasNamespace) ? context.getProperty("ns:" + parts[0]).evaluateAttributeExpressions(flowFile).getValue() : "";
                elementObject.addProperty("name", name);
                elementObject.addProperty("ns", ns);
                rangeObject.add("element", elementObject);
                break;
            case IndexTypes.JSON_PROPERTY_STR:
                rangeObject.addProperty("json-property", stateIndexValue);
                break;
            case IndexTypes.PATH_STR:
                JsonObject pathObject = new JsonObject();
                pathObject.addProperty("text", stateIndexValue);
                JsonObject namespacesObject = new JsonObject();
                for (PropertyDescriptor propertyDesc : propertiesByPrefix.get("ns")) {
                    namespacesObject.addProperty(propertyDesc.getName().substring(3),
                        context.getProperty(propertyDesc).evaluateAttributeExpressions(flowFile).getValue());
                }
                pathObject.add("namespaces", namespacesObject);
                rangeObject.add("path-index", pathObject);
                break;
            default:
                break;
        }
        return rootObject.toString();
    }

    public static class ReturnTypes {
        public static final String URIS_ONLY_STR = "URIs Only";
        public static final AllowableValue URIS_ONLY = new AllowableValue(URIS_ONLY_STR, URIS_ONLY_STR,
            "Each FlowFile will have its 'filename' attribute set to a URI matching the query");
        public static final String DOCUMENTS_STR = "Documents";
        public static final AllowableValue DOCUMENTS = new AllowableValue(DOCUMENTS_STR, DOCUMENTS_STR,
            "Each FlowFile will have a document matching the query as its content");
        public static final String DOCUMENTS_AND_META_STR = "Documents + Metadata";
        public static final AllowableValue DOCUMENTS_AND_META = new AllowableValue(DOCUMENTS_AND_META_STR, DOCUMENTS_AND_META_STR,
            "Each FlowFile will have a documenting matching the query as its content and attributes added containing " +
                "metadata for the document");
        public static final String META_STR = "Metadata";
        public static final AllowableValue META = new AllowableValue(META_STR, META_STR,
            "Each FlowFile will have attributes added containing metadata for a document matching the query");

        public static final AllowableValue[] allValues = new AllowableValue[]{URIS_ONLY, DOCUMENTS,
            DOCUMENTS_AND_META, META};

    }

    public static class IndexTypes {
        public static final String ELEMENT_STR = "Element Index";
        public static final AllowableValue ELEMENT = new AllowableValue(ELEMENT_STR, ELEMENT_STR,
            "Index on an element. Namespaces can be defined with dynamic properties prefixed with 'ns:'. For example, " +
                "for a State Index value of 'xhtml:title', include a dynamic property named 'ns:xhtml' with a " +
                "value of e.g. 'http://www.w3.org/1999/xhtml'.");
        public static final String JSON_PROPERTY_STR = "JSON Property Index";
        public static final AllowableValue JSON_PROPERTY = new AllowableValue(JSON_PROPERTY_STR, JSON_PROPERTY_STR,
            "Index on a JSON property. A State Index value will simply have the name of a JSON property.");
        public static final String PATH_STR = "Path Index";
        public static final AllowableValue PATH = new AllowableValue(PATH_STR, PATH_STR,
            "Index on a path. Namespaces can be defined with dynamic properties prefixed with 'ns:'. For " +
                "example, for a State Index value of '/xhtml:html/xhtml:title', include a dynamic property named " +
                "'ns:xhtml' with a value of e.g. 'http://www.w3.org/1999/xhtml'.");

        public static final AllowableValue[] allValues = new AllowableValue[]{ELEMENT, JSON_PROPERTY, PATH};

    }
}
