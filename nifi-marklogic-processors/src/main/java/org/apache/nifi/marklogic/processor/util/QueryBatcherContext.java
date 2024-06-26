package org.apache.nifi.marklogic.processor.util;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.query.QueryDefinition;

public class QueryBatcherContext {

    private final DataMovementManager manager;
    private final QueryBatcher batcher;
    private final QueryDefinition definition;
    private final String queryRepresentation;

    public QueryBatcherContext(DataMovementManager manager, QueryBatcher batcher, QueryDefinition definition, String queryRepresentation) {
        this.manager = manager;
        this.batcher = batcher;
        this.definition = definition;
        this.queryRepresentation = queryRepresentation;
    }

    public DataMovementManager getManager() { return manager; }
    public QueryBatcher getBatcher() { return batcher; }
    public QueryDefinition getDefinition() { return definition; }
    public String getQueryRepresentation() { return queryRepresentation; }
}