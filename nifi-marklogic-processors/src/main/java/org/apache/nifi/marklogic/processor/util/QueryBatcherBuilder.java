package org.apache.nifi.marklogic.processor.util;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.*;
import org.apache.nifi.util.EscapeUtils;
import org.apache.nifi.util.Tuple;

import java.util.regex.Pattern;

/**
 * This class was extracted from {@code QueryMarkLogic} to both simplify that class and also make it easier to
 * understand the logic for constructing a {@code QueryBatcher}. It is not yet intended for use outside this project
 * as it still depends on a couple NiFi classes. Those dependencies could be easily removed though if necessary.
 */
public class QueryBatcherBuilder {

    private DatabaseClient client;
    private final static Pattern searchJson = Pattern.compile("^\\s*\\{\\s*\"(search|ctsquery)\"\\s*:\\s*");
    private final static Pattern searchJsonEnd = Pattern.compile("\\}\\s*$");


    public QueryBatcherBuilder(DatabaseClient client) {
        this.client = client;
    }

    /**
     * Constructs a {@code QueryBatcher}, along with its associated {@code DataMovementManager}, based on the given
     * query type and value. If {@code stateRangeIndexQuery} is not null, then it will be combined with the query
     * constructed on the query type and value.
     *
     * @param queryTypeAndValue
     * @param stateRangeIndexQuery can be null
     * @return
     */
    public QueryBatcherContext newQueryBatcher(QueryTypeAndValue queryTypeAndValue, RangeIndexQuery stateRangeIndexQuery) {
        DataMovementManager dataMovementManager = client.newDataMovementManager();
        QueryManager queryManager = client.newQueryManager();

        Tuple<QueryDefinition, Format> queryAndFormat = buildQueryDefinitionAndFormat(queryManager, queryTypeAndValue);
        final QueryDefinition queryDef = queryAndFormat.getKey();

        QueryBatcher queryBatcher;
        if (stateRangeIndexQuery != null) {
            String rawCombinedQuery = buildRawCombinedQueryWithStateQuery(queryAndFormat, queryTypeAndValue, stateRangeIndexQuery);
            StringHandle handle = new StringHandle(rawCombinedQuery).withFormat(queryAndFormat.getValue());
            RawCombinedQueryDefinition query = queryManager.newRawCombinedQueryDefinition(handle);
            queryBatcher = dataMovementManager.newQueryBatcher(query);
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
        
        return new QueryBatcherContext(dataMovementManager, queryBatcher, queryDef);
    }

    private Tuple<QueryDefinition, Format> buildQueryDefinitionAndFormat(QueryManager queryManager, QueryTypeAndValue queryTypeAndValue) {
        QueryDefinition queryDef;
        final String queryValue = queryTypeAndValue.value;
        switch (queryTypeAndValue.type) {
            case QueryTypes.COLLECTION_STR:
                String[] collections = queryValue.split(",");
                queryDef = queryManager.newStructuredQueryBuilder().collection(collections);
                return new Tuple<>(queryDef, Format.XML);
            case QueryTypes.COMBINED_JSON_STR:
                queryDef = queryManager.newRawCombinedQueryDefinition(new StringHandle(queryValue).withFormat(Format.JSON));
                return new Tuple<>(queryDef, Format.JSON);
            case QueryTypes.COMBINED_XML_STR:
                queryDef = queryManager.newRawCombinedQueryDefinition(new StringHandle(queryValue).withFormat(Format.XML));
                return new Tuple<>(queryDef, Format.XML);
            case QueryTypes.STRUCTURED_JSON_STR:
                queryDef = queryManager.newRawStructuredQueryDefinition(new StringHandle(queryValue).withFormat(Format.JSON));
                return new Tuple<>(queryDef, Format.JSON);
            case QueryTypes.STRUCTURED_XML_STR:
                queryDef = queryManager.newRawStructuredQueryDefinition(new StringHandle(queryValue).withFormat(Format.XML));
                return new Tuple<>(queryDef, Format.XML);
            default:
                queryDef = queryManager.newStringDefinition().withCriteria(queryValue);
                return new Tuple<>(queryDef, Format.XML);
        }
    }

    /**
     * Combines the user's NiFi-properties-based query with the given state query into a string representing the
     * raw combined query.
     *
     * @param queryDefAndFormat
     * @param queryTypeAndValue
     * @param stateRangeIndexQuery
     * @return
     */
    private String buildRawCombinedQueryWithStateQuery(Tuple<QueryDefinition, Format> queryDefAndFormat,
                                                       QueryTypeAndValue queryTypeAndValue,
                                                       RangeIndexQuery stateRangeIndexQuery) {
        final QueryDefinition queryDef = queryDefAndFormat.getKey();
        final Format format = queryDefAndFormat.getValue();
        final String queryValue = queryTypeAndValue.value;

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
                    .append(stateRangeIndexQuery.toStructuredQuery(format))
                    .append(",")
                    .append(queryBody)
                    .append("]}");
            } else {
                rawCombinedQueryBuilder
                    .append("<query>")
                    .append(stateRangeIndexQuery.toStructuredQuery(format))
                    .append(queryBody)
                    .append("</query>");
            }
            rawCombinedQueryBuilder.append((format == Format.JSON) ? "} }" : "</search>");
        } else if (queryDef instanceof RawCombinedQueryDefinition) {
            if (format == Format.XML) {
                rawCombinedQueryBuilder
                    .append("<cts:and-query xmlns:cts=\"http://marklogic.com/cts\">")
                    .append(queryValue)
                    .append(stateRangeIndexQuery.toCtsQuery(format))
                    .append("</cts:and-query>");
            } else {
                rawCombinedQueryBuilder
                    .append("{\"ctsquery\": { \"andQuery\": { \"queries\": [ ")
                    .append(stateRangeIndexQuery.toCtsQuery(format))
                    .append(",")
                    .append(prepQueryToCombineJSON(queryValue))
                    .append("]}}}");
            }
        } else if (queryDef instanceof StringQueryDefinition) {
            rawCombinedQueryBuilder
                .append("<search  xmlns=\"http://marklogic.com/appservices/search\">")
                .append(stateRangeIndexQuery.toStructuredQuery(format))
                .append("<qtext>").append(EscapeUtils.escapeHtml(queryValue))
                .append("</qtext></search>");
        }
        return rawCombinedQueryBuilder.toString();
    }

    private String prepQueryToCombineJSON(String json) {
        if (searchJson.matcher(json).lookingAt()) {
            String newJSON = searchJsonEnd.matcher(searchJson.matcher(json).replaceFirst("")).replaceFirst("");
            return (newJSON.equals(json)) ? json : prepQueryToCombineJSON(newJSON);
        } else {
            return json;
        }
    }

    /**
     * Simple tuple-like class for ferrying around the user's chosen query type and query value.
     */
    public static class QueryTypeAndValue {

        private String type;
        private String value;

        public QueryTypeAndValue(String type, String value) {
            this.type = type;
            this.value = value;
        }

        public String getType() {
            return type;
        }

        public String getValue() {
            return value;
        }
    }
}
