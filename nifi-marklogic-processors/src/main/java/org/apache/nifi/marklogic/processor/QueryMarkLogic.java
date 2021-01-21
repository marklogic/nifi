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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.marklogic.processor.util.RangeIndexQuery;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.EscapeUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.ExportListener;
import com.marklogic.client.datamovement.JobReport;
import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.datamovement.QueryBatchListener;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.datamovement.impl.JobReportImpl;
import com.marklogic.client.document.DocumentManager.Metadata;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.ValuesHandle;
import com.marklogic.client.query.AggregateResult;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.RawStructuredQueryDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryBuilder.Operator;
import com.marklogic.client.query.StructuredQueryDefinition;
import com.marklogic.client.query.ValuesDefinition;
import com.marklogic.client.util.EditableNamespaceContext;

@Tags({ "MarkLogic", "Get", "Query", "Read" })
@InputRequirement(Requirement.INPUT_ALLOWED)
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@CapabilityDescription("Creates FlowFiles from batches of documents, matching the given criteria,"
        + " retrieved from a MarkLogic server using the MarkLogic Data Movement SDK (DMSDK)")
@DynamicProperty(name = "trans: Server transform parameter name, ns: Namespace prefix for XPath or element names",
    value = "trans: Value of the server transform parameter, ns: Namespace value associated with prefix",
    description = "Depending on the property prefix, routes data to transform or maps to a namespace prefix.",
    expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The filename is set to the uri of the document retrieved from MarkLogic") })
@Stateful(description = "Can keep state of a range index value to restrict future queries.", scopes = { Scope.CLUSTER })
public class QueryMarkLogic extends AbstractMarkLogicProcessor {

    public static final PropertyDescriptor CONSISTENT_SNAPSHOT = new PropertyDescriptor.Builder()
            .name("Consistent Snapshot").displayName("Consistent Snapshot").defaultValue("true")
            .description("Boolean used to indicate that the matching documents were retrieved from a "
                    + "consistent snapshot")
            .required(true).addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor.Builder().name("Return Type")
            .displayName("Return Type").defaultValue(ReturnTypes.DOCUMENTS.getValue())
            .description("Determines what gets retrieved by query").required(true)
            .allowableValues(ReturnTypes.allValues).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder().name("Query").displayName("Query")
            .description("Query text that corresponds with the selected Query Type").required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor QUERY_TYPE = new PropertyDescriptor.Builder().name("Query Type")
            .displayName("Query Type").description("Type of query that will be used to retrieve data from MarkLogic")
            .required(true).allowableValues(QueryTypes.allValues).defaultValue(QueryTypes.COMBINED_JSON.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor STATE_INDEX = new PropertyDescriptor.Builder().name("State Index")
            .displayName("State Index")
            .description("Definition of the index which will be used to keep state to restrict future calls")
            .required(false).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID).build();

    public static final PropertyDescriptor STATE_INDEX_TYPE = new PropertyDescriptor.Builder().name("State Index Type")
            .displayName("State Index Type").description("Type of index to determine state for next set of documents.")
            .required(true).expressionLanguageSupported(ExpressionLanguageScope.NONE).allowableValues(IndexTypes.allValues).defaultValue(IndexTypes.JSON_PROPERTY.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor COLLECTIONS = new PropertyDescriptor.Builder().name("Collections")
            .displayName("Collections")
            .description(
                    "**Deprecated: Use Query Type and Query** Comma-separated list of collections to query from a MarkLogic server")
            .required(false).addValidator(Validator.VALID).build();

    protected static final Relationship SUCCESS = new Relationship.Builder().name("success")
            .description("For each record retrieved for the query, a FlowFile is sent to this relationship").build();

    protected static final Relationship FAILURE = new Relationship.Builder().name("failure")
            .description("If an error occurs while retrieving a batch of records for a query, a FlowFile will be sent to this relationship").build();

    protected static final Relationship ORIGINAL = new Relationship.Builder().name("original")
            .description("If this processor receives a FlowFile, it will be routed to this relationship").build();

    protected QueryBatcher queryBatcher;

    protected volatile AtomicLong serverTimestamp = new AtomicLong(0);
    protected volatile String queryState = null;

    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);

        List<PropertyDescriptor> list = new ArrayList<>(properties);
        list.add(CONSISTENT_SNAPSHOT);
        list.add(QUERY);
        list.add(QUERY_TYPE);
        list.add(RETURN_TYPE);
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

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        Set<ValidationResult> validationResultSet = new HashSet<>();
        String collections = validationContext.getProperty(COLLECTIONS).getValue();
        String query = validationContext.getProperty(QUERY).getValue();
        if (collections == null && query == null) {
            validationResultSet.add(new ValidationResult.Builder().subject("Query").valid(false)
                    .explanation("The Query value must be set. "
                            + "The deprecated Collections property will be migrated appropriately.")
                    .build());
        }
        return validationResultSet;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory)
            throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        onTrigger(context, session);
    }

    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        super.populatePropertiesByPrefix(context);
        try {
            final FlowFile incomingFlowFile = context.hasIncomingConnection() ? session.get() : null;

            StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
            DatabaseClient client = getDatabaseClient(context);
            DataMovementManager dataMovementManager = client.newDataMovementManager();
            queryBatcher = createQueryBatcherWithQueryCriteria(context, session, incomingFlowFile, getDatabaseClient(context),
                    dataMovementManager);
            if (context.getProperty(BATCH_SIZE).asInteger() != null)
                queryBatcher.withBatchSize(context.getProperty(BATCH_SIZE).asInteger());
            if (context.getProperty(THREAD_COUNT).asInteger() != null)
                queryBatcher.withThreadCount(context.getProperty(THREAD_COUNT).asInteger());
            final boolean consistentSnapshot;
            if (context.getProperty(CONSISTENT_SNAPSHOT).asBoolean() != null
                    && !context.getProperty(CONSISTENT_SNAPSHOT).asBoolean()) {
                consistentSnapshot = false;
            } else {
                queryBatcher.withConsistentSnapshot();
                consistentSnapshot = true;
            }
            QueryBatchListener batchListener = buildQueryBatchListener(context, session, consistentSnapshot);
            queryBatcher.onJobCompletion((batcher) -> {
                JobReport report = new JobReportImpl(batcher);
                if (report.getSuccessEventsCount() == 0) {
                    context.yield();
                }
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("ML Query Job Complete [Success Count=" + report.getSuccessEventsCount()
                            + "] [Failure Count=" + report.getFailureEventsCount() + "]");
                }
                if (report.getFailureBatchesCount() == 0 && context.getProperty(STATE_INDEX) != null
                        && context.getProperty(STATE_INDEX).isSet()) {
                    QueryManager queryMgr = client.newQueryManager();
                    ValuesDefinition valuesDef = queryMgr.newValuesDefinition("state");
                    RawCombinedQueryDefinition qDef = queryMgr.newRawCombinedQueryDefinition(
                            handleForQuery(buildStateConstraintOptions(context, incomingFlowFile), Format.JSON));
                    valuesDef.setQueryDefinition(qDef);
                    valuesDef.setAggregate("max");
                    ValuesHandle valuesResult = new ValuesHandle();
                    valuesResult.setPointInTimeQueryTimestamp(serverTimestamp.get());
                    valuesResult.setQueryCriteria(valuesDef);
                    valuesResult = queryMgr.values(valuesDef, valuesResult);
                    AggregateResult result = valuesResult.getAggregate("max");
                    queryState = result.getValue();
                    Map<String, String> alterMap = new HashMap<String, String>(stateMap.toMap());
                    alterMap.put("queryState", queryState);
                    try {
                        context.getStateManager().setState(alterMap, Scope.CLUSTER);
                    } catch (IOException e) {
                        getLogger().error("{} Failed to store state", new Object[] { this });
                    } finally {
                        queryState = null;
                    }
                }
            });
            queryBatcher.onUrisReady(batchListener);
            queryBatcher.onUrisReady((batch) -> {
                if (batch.getJobBatchNumber() == 1) {
                    serverTimestamp.set(batch.getServerTimestamp());
                }
            });

            // Ensure that this FF has a destination so that session.commit() does not throw an error
            if (incomingFlowFile != null) {
                session.transfer(incomingFlowFile, ORIGINAL);
            }

            dataMovementManager.startJob(queryBatcher);
            queryBatcher.awaitCompletion();
            dataMovementManager.stopJob(queryBatcher);
            session.commit();
        } catch (final Throwable t) {
            context.yield();
            this.handleThrowable(t, session);
        }
    }

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
        String stateIndexTypeValue = context.getProperty(STATE_INDEX_TYPE).getValue();
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

    protected QueryBatchListener buildQueryBatchListener(final ProcessContext context, final ProcessSession session,
            final boolean consistentSnapshot) {
        final boolean retrieveFullDocument;
        if (context.getProperty(RETURN_TYPE) != null
                && (ReturnTypes.DOCUMENTS_STR.equals(context.getProperty(RETURN_TYPE).getValue())
                        || ReturnTypes.DOCUMENTS_AND_META_STR.equals(context.getProperty(RETURN_TYPE).getValue()))) {
            retrieveFullDocument = true;
        } else {
            retrieveFullDocument = false;
        }
        final boolean retrieveMetadata;
        if (context.getProperty(RETURN_TYPE) != null && (ReturnTypes.META.getValue()
                .equals(context.getProperty(RETURN_TYPE).getValue())
                || ReturnTypes.DOCUMENTS_AND_META.getValue().equals(context.getProperty(RETURN_TYPE).getValue()))) {
            retrieveMetadata = true;
        } else {
            retrieveMetadata = false;
        }

        QueryBatchListener batchListener = null;

        if (retrieveFullDocument) {
            ExportListener exportListener = new ExportListener().onDocumentReady(doc -> {
                synchronized (session) {
                    final FlowFile flowFile = session.write(session.create(),
                            out -> out.write(doc.getContent(new BytesHandle()).get()));
                    if (retrieveMetadata) {
                        DocumentMetadataHandle metaHandle = doc.getMetadata(new DocumentMetadataHandle());
                        metaHandle.getMetadataValues().forEach((metaKey, metaValue) -> {
                            session.putAttribute(flowFile, "meta:" + metaKey, metaValue);
                        });
                        metaHandle.getProperties().forEach((qname, propertyValue) -> {
                            session.putAttribute(flowFile, "property:" + qname.toString(), propertyValue.toString());
                        });
                    }
                    session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), doc.getUri());
                    session.transfer(flowFile, SUCCESS);
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("Routing " + doc.getUri() + " to " + SUCCESS.getName());
                    }
                }
            });
            if (retrieveMetadata) {
                exportListener.withMetadataCategory(Metadata.ALL);
            }
            if (consistentSnapshot) {
                exportListener.withConsistentSnapshot();
            }
            ServerTransform transform = this.buildServerTransform(context);
            if (transform != null) {
                exportListener.withTransform(transform);
            }
            batchListener = exportListener;
        } else {
            batchListener = new QueryBatchListener() {
                @Override
                public void processEvent(QueryBatch batch) {
                    synchronized (session) {
                        Arrays.stream(batch.getItems()).forEach((uri) -> {
                            FlowFile flowFile = session.create();
                            session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), uri);
                            if (retrieveMetadata) {
                                DocumentMetadataHandle metaHandle = new DocumentMetadataHandle();
                                if (consistentSnapshot) {
                                    metaHandle.setServerTimestamp(batch.getServerTimestamp());
                                }
                                batch.getClient().newDocumentManager().readMetadata(uri, metaHandle);
                                metaHandle.getMetadataValues().forEach((metaKey, metaValue) -> {
                                    session.putAttribute(flowFile, "meta:" + metaKey, metaValue);
                                });
                                metaHandle.getProperties().forEach((qname, propertyValue) -> {
                                    session.putAttribute(flowFile, "property:" + qname.toString(),
                                            propertyValue.toString());
                                });
                            }
                            session.transfer(flowFile, SUCCESS);
                            if (getLogger().isDebugEnabled()) {
                                getLogger().debug("Routing " + uri + " to " + SUCCESS.getName());
                            }
                        });
                        session.commit();
                    }
                }
            };
        }
        return batchListener;
    }

    private QueryBatcher createQueryBatcherWithQueryCriteria(ProcessContext context, ProcessSession session,
            FlowFile incomingFlowFile, DatabaseClient databaseClient, DataMovementManager dataMovementManager) {
        final PropertyValue queryProperty = context.getProperty(QUERY);
        final String queryValue;
        final String queryTypeValue;
        QueryDefinition queryDef = null;

        // Gracefully migrate old Collections templates
        String collectionsValue = context.getProperty(COLLECTIONS).getValue();
        if (!(collectionsValue == null || "".equals(collectionsValue))) {
            queryValue = collectionsValue;
            queryTypeValue = QueryTypes.COLLECTION.getValue();
        } else {
            queryValue = queryProperty.evaluateAttributeExpressions(incomingFlowFile).getValue();
            queryTypeValue = context.getProperty(QUERY_TYPE).getValue();
        }
        final QueryManager queryManager = databaseClient.newQueryManager();
        StructuredQueryBuilder queryBuilder = queryManager.newStructuredQueryBuilder();
        RangeIndexQuery stateQuery = null;
        StateMap stateMap = null;
        try {
            stateMap = context.getStateManager().getState(Scope.CLUSTER);
        } catch (IOException e) {
            getLogger().error("Failed to get state map", new Object[] { this }, e);
        }
        queryState = (stateMap != null) ? stateMap.get("queryState") : null;
        if (!(queryState == null || "".equals(queryState)) && context.getProperty(STATE_INDEX) != null
                && context.getProperty(STATE_INDEX).isSet()) {
            stateQuery = buildStateQuery(queryBuilder, context, incomingFlowFile);
        }
        Format format = Format.XML;
        if (queryValue != null) {
            switch (queryTypeValue) {
            case QueryTypes.COLLECTION_STR:
                String[] collections = getArrayFromCommaSeparatedString(queryValue);
                if (collections != null) {
                    queryDef = queryBuilder.collection(collections);
                }
                format = Format.XML;
                break;
            case QueryTypes.COMBINED_JSON_STR:
                queryDef = queryManager.newRawCombinedQueryDefinition(handleForQuery(queryValue, Format.JSON));
                format = Format.JSON;
                break;
            case QueryTypes.COMBINED_XML_STR:
                queryDef = queryManager.newRawCombinedQueryDefinition(handleForQuery(queryValue, Format.XML));
                format = Format.XML;
                break;
            case QueryTypes.STRING_STR:
                queryDef = queryManager.newStringDefinition().withCriteria(queryValue);
                format = Format.XML;
                break;
            case QueryTypes.STRUCTURED_JSON_STR:
                queryDef = queryManager.newRawStructuredQueryDefinition(handleForQuery(queryValue, Format.JSON));
                format = Format.JSON;
                break;
            case QueryTypes.STRUCTURED_XML_STR:
                queryDef = queryManager.newRawStructuredQueryDefinition(handleForQuery(queryValue, Format.XML));
                break;
            default:
                throw new IllegalStateException("No valid Query type selected!");
            }
        }
        if (stateQuery != null) {
            StringBuilder rawCombinedQueryBuilder = new StringBuilder();
            if (queryDef instanceof StructuredQueryDefinition || queryDef instanceof RawStructuredQueryDefinition) {
                rawCombinedQueryBuilder.append((format == Format.JSON) ? "{ \"search\": { "
                        : "<search xmlns=\"http://marklogic.com/appservices/search\">");
                String queryBody;
                if (queryDef instanceof StructuredQueryDefinition) {
                    queryBody = ((StructuredQueryDefinition) queryDef).serialize();
                } else {
                    queryBody = queryValue;
                }
                if (format == Format.JSON) {
                    rawCombinedQueryBuilder
                        .append("\"query\": {\"queries\": [ ")
                        .append(stateQuery.toStructuredQuery(format))
                        .append(",")
                        .append(queryBody)
                        .append("]}");
                } else {
                    rawCombinedQueryBuilder
                        .append("<query>")
                        .append(stateQuery.toStructuredQuery(format))
                        .append(queryBody)
                        .append("</query>");
                }
                rawCombinedQueryBuilder.append((format == Format.JSON) ? "} }" : "</search>");
            } else if (queryDef instanceof RawCombinedQueryDefinition) {
                if (format == Format.XML) {
                    rawCombinedQueryBuilder
                        .append("<cts:and-query xmlns:cts=\"http://marklogic.com/cts\">")
                        .append(queryValue)
                        .append(stateQuery.toCtsQuery(format))
                        .append("</cts:and-query>");
                } else {
                   rawCombinedQueryBuilder
                        .append("{\"ctsquery\": { \"andQuery\": { \"queries\": [ ")
                        .append(stateQuery.toCtsQuery(format))
                        .append(",")
                        .append(prepQueryToCombineJSON(queryValue))
                        .append("]}}}");
                }
            } else if (queryDef instanceof StringQueryDefinition) {
                rawCombinedQueryBuilder
                    .append("<search  xmlns=\"http://marklogic.com/appservices/search\">")
                    .append(stateQuery.toStructuredQuery(format))
                    .append("<qtext>").append(EscapeUtils.escapeHtml(queryValue))
                    .append("</qtext></search>");
            }
            RawCombinedQueryDefinition finalQuery = queryManager
                    .newRawCombinedQueryDefinition(handleForQuery(rawCombinedQueryBuilder.toString(), format));
            queryBatcher = dataMovementManager.newQueryBatcher(finalQuery);
        } else {
            if (queryDef instanceof RawCombinedQueryDefinition) {
                queryBatcher = dataMovementManager.newQueryBatcher((RawCombinedQueryDefinition) queryDef);
            } else if (queryDef instanceof RawStructuredQueryDefinition) {
                queryBatcher = dataMovementManager.newQueryBatcher((RawStructuredQueryDefinition) queryDef);
            } else if (queryDef instanceof StructuredQueryDefinition) {
                queryBatcher = dataMovementManager.newQueryBatcher((StructuredQueryDefinition) queryDef);
            } else {
                queryBatcher = dataMovementManager.newQueryBatcher((StringQueryDefinition) queryDef);
            }
        }
        if (queryBatcher == null) {
            throw new IllegalStateException("No valid Query criteria specified!");
        }
        queryBatcher.onQueryFailure(exception -> {
            getLogger().error("Query failure: " + exception.getMessage());
            FlowFile failureFlowFile = incomingFlowFile != null ? session.penalize(incomingFlowFile) : session.create();
            session.transfer(failureFlowFile, FAILURE);
            session.commit();
            context.yield();
        });
        return queryBatcher;
    }

    Pattern searchJson = Pattern.compile("^\\s*\\{\\s*\"(search|ctsquery)\"\\s*:\\s*");
    Pattern searchJsonEnd = Pattern.compile("\\}\\s*$");

    String prepQueryToCombineJSON(String json) {
        if (searchJson.matcher(json).lookingAt()) {
            String newJSON = searchJsonEnd.matcher(searchJson.matcher(json).replaceFirst("")).replaceFirst("");
            return (newJSON.equals(json))? json: prepQueryToCombineJSON(newJSON);
        } else {
            return json;
        }
    }

    RangeIndexQuery buildStateQuery(StructuredQueryBuilder queryBuilder, final ProcessContext context,
            final FlowFile incomingFlowFile) {
        String stateIndexValue = context.getProperty(STATE_INDEX).evaluateAttributeExpressions(incomingFlowFile).getValue();
        String stateIndexTypeValue = context.getProperty(STATE_INDEX_TYPE).getValue();
        List<PropertyDescriptor> namespaceProperties = propertiesByPrefix.get("ns");
        EditableNamespaceContext namespaces = new EditableNamespaceContext();
        if (namespaceProperties != null) {
            for (PropertyDescriptor propertyDesc : namespaceProperties) {
                namespaces.put(propertyDesc.getName().substring(3),
                        context.getProperty(propertyDesc).evaluateAttributeExpressions(incomingFlowFile).getValue());
            }
        }
        queryBuilder.setNamespaces(namespaces);
        return new RangeIndexQuery(queryBuilder, stateIndexTypeValue, stateIndexValue, "xs:dateTime", Operator.GT,
                queryState);
    }

    QueryBatcher getQueryBatcher() {
        return this.queryBatcher;
    }

    StringHandle handleForQuery(String queryValue, Format format) {
        StringHandle handle = new StringHandle().withFormat(format).with(queryValue);
        return handle;
    }

    public static class QueryTypes extends AllowableValuesSet {
        public static final String COLLECTION_STR = "Collection Query";
        public static final AllowableValue COLLECTION = new AllowableValue(COLLECTION_STR, COLLECTION_STR,
                "Comma-separated list of collections to query from a MarkLogic server");
        public static final String COMBINED_JSON_STR = "Combined Query (JSON)";
        public static final AllowableValue COMBINED_JSON = new AllowableValue(COMBINED_JSON_STR, COMBINED_JSON_STR,
                "Combine a string or structured query with dynamic query options (Allows JSON serialized cts queries)");
        public static final String COMBINED_XML_STR = "Combined Query (XML)";
        public static final AllowableValue COMBINED_XML = new AllowableValue(COMBINED_XML_STR, COMBINED_XML_STR,
                "Combine a string or structured query with dynamic query options (Allows XML serialized cts queries)");
        public static final String STRING_STR = "String Query";
        public static final AllowableValue STRING = new AllowableValue(STRING_STR, STRING_STR,
                "A Google-style query string to search documents and metadata.");
        public static final String STRUCTURED_JSON_STR = "Structured Query (JSON)";
        public static final AllowableValue STRUCTURED_JSON = new AllowableValue(STRUCTURED_JSON_STR,
                STRUCTURED_JSON_STR,
                "A simple and easy way to construct queries as a JSON structure, allowing you to manipulate complex queries");
        public static final String STRUCTURED_XML_STR = "Structured Query (XML)";
        public static final AllowableValue STRUCTURED_XML = new AllowableValue(STRUCTURED_XML_STR, STRUCTURED_XML_STR,
                "A simple and easy way to construct queries as a XML structure, allowing you to manipulate complex queries");

        public static final AllowableValue[] allValues = new AllowableValue[] { COLLECTION, COMBINED_JSON, COMBINED_XML,
                STRING, STRUCTURED_JSON, STRUCTURED_XML };

    }

    public static class ReturnTypes extends AllowableValuesSet {
        public static final String URIS_ONLY_STR = "URIs Only";
        public static final AllowableValue URIS_ONLY = new AllowableValue(URIS_ONLY_STR, URIS_ONLY_STR,
                "Only return document URIs for matching documents in FlowFile attribute");
        public static final String DOCUMENTS_STR = "Documents";
        public static final AllowableValue DOCUMENTS = new AllowableValue(DOCUMENTS_STR, DOCUMENTS_STR,
                "Return documents in FlowFile contents");
        public static final String DOCUMENTS_AND_META_STR = "Documents + Metadata";
        public static final AllowableValue DOCUMENTS_AND_META = new AllowableValue(DOCUMENTS_AND_META_STR,
                DOCUMENTS_AND_META_STR, "Return documents in FlowFile contents and metadata in FlowFile attributes");
        public static final String META_STR = "Metadata";
        public static final AllowableValue META = new AllowableValue(META_STR, META_STR,
                "Return metadata in FlowFile attributes");

        public static final AllowableValue[] allValues = new AllowableValue[] { URIS_ONLY, DOCUMENTS,
                DOCUMENTS_AND_META, META };

    }

    public static class IndexTypes extends AllowableValuesSet {
        public static final String ELEMENT_STR = "Element Index";
        public static final AllowableValue ELEMENT = new AllowableValue(ELEMENT_STR, ELEMENT_STR,
                "Index on an element. (Namespaces can be defined with dynamic properties prefixed with 'ns:'.)");
        public static final String JSON_PROPERTY_STR = "JSON Property Index";
        public static final AllowableValue JSON_PROPERTY = new AllowableValue(JSON_PROPERTY_STR, JSON_PROPERTY_STR,
                "Index on a JSON property.");
        public static final String PATH_STR = "Path Index";
        public static final AllowableValue PATH = new AllowableValue(PATH_STR, PATH_STR,
                "Index on a Path. (Namespaces can be defined with dynamic properties prefixed with 'ns:'.)");

        public static final AllowableValue[] allValues = new AllowableValue[] { ELEMENT, JSON_PROPERTY, PATH };

    }
}
