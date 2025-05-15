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

import com.marklogic.client.datamovement.QueryBatchListener;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;

import java.util.*;

@Tags({"MarkLogic", "Delete"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("Creates FlowFiles from batches of documents, matching the given criteria,"
    + " deleted from a MarkLogic server using the MarkLogic Data Movement SDK (DMSDK). Requires a MarkLogic user " +
    "with the 'rest-reader' privilege to read documents and the 'rest-writer' privilege in delete documents.")
@DynamicProperties({
    @DynamicProperty(
        name = "ns:{prefix}",
        value = "A namespace URI",
        description = "Define namespace prefixes and URIs that can be used to construct State Index values when " +
            "State Index type is either ELEMENT or PATH",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
})
public class DeleteMarkLogic extends QueryMarkLogic {

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return null;
    }

    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> list = new ArrayList<>();
        list.add(DATABASE_CLIENT_SERVICE);
        list.add(BATCH_SIZE);
        list.add(THREAD_COUNT);
        list.add(QUERY);
        list.add(QUERY_TYPE);
        list.add(STATE_INDEX);
        list.add(STATE_INDEX_TYPE);
        properties = Collections.unmodifiableList(list);

        final Set<Relationship> set = new HashSet<>();
        set.add(SUCCESS);
        set.add(FAILURE);
        set.add(ORIGINAL);
        relationships = Collections.unmodifiableSet(set);
    }

    /**
     * Overrides the behavior in the parent class for how each batch of URIs should be processed.
     *
     * @param context
     * @param session
     * @param incomingAttributes
     * @return
     */
    @Override
    protected QueryBatchListener buildQueryBatchListener(final ProcessContext context, final ProcessSession session,
                                                         Map<String, String> incomingAttributes) {
        return batch -> {
            try {
                batch.getClient().newDocumentManager().delete(batch.getItems());
                transferBatch(session, incomingAttributes, batch, SUCCESS, null);
            } catch (Throwable t) {
                getLogger().error("Unable to delete batch of URIs; cause: " + t.getMessage());
                transferBatch(session, incomingAttributes, batch, FAILURE, t);
            }
        };
    }
}
