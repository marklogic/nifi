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
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import com.marklogic.client.datamovement.DeleteListener;
import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.datamovement.QueryBatchListener;

@Tags({ "MarkLogic", "Delete" })
@InputRequirement(Requirement.INPUT_ALLOWED)
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@CapabilityDescription("Creates FlowFiles from batches of documents, matching the given criteria,"
        + " deleted from a MarkLogic server using the MarkLogic Data Movement SDK (DMSDK)")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The filename is set to the uri of the document deleted from MarkLogic") })
@Stateful(description = "Can keep state of a range index value to restrict future queries.", scopes = { Scope.CLUSTER })
public class DeleteMarkLogic extends QueryMarkLogic {

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return null;
    }

    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(DATABASE_CLIENT_SERVICE);
        list.add(BATCH_SIZE);
        list.add(THREAD_COUNT);
        list.add(QUERY);
        list.add(QUERY_TYPE);
        list.add(STATE_INDEX);
        list.add(STATE_INDEX_TYPE);
        properties = Collections.unmodifiableList(list);
        Set<Relationship> set = new HashSet<>();
        set.add(SUCCESS);
        set.add(FAILURE);
        relationships = Collections.unmodifiableSet(set);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory)
            throws ProcessException {
        super.onTrigger(context, sessionFactory);
    }

    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        super.onTrigger(context, session);
    }

    protected QueryBatchListener buildQueryBatchListener(final ProcessContext context, final ProcessSession session, final boolean consistentSnapshot) {
        return new NiFiDeleteListener(session).onFailure((batch, throwable) -> {
            synchronized(session) {
                getLogger().error("Error deleting batch", throwable);
                for (String uri : batch.getItems()) {
                    FlowFile flowFile = session.create();
                    session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), uri);
                    session.transfer(flowFile, FAILURE);
                }
                session.commit();
                context.yield();
            }
        });
    }

    private class NiFiDeleteListener extends DeleteListener {
        private final ProcessSession session;

        private NiFiDeleteListener(final ProcessSession session) {
            super();
            this.session = session;
        }

        @Override
        public void processEvent(QueryBatch batch) {
            super.processEvent(batch);
            synchronized(session) {
                for (String uri : batch.getItems()) {
                    FlowFile flowFile = session.create();
                    session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), uri);
                    session.transfer(flowFile, SUCCESS);
                }
                session.commit();
            }
        }
    }
}
