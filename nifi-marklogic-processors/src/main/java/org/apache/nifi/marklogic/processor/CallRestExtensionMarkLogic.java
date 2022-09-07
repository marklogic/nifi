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

import com.marklogic.client.extensions.ResourceServices;
import com.marklogic.client.io.BytesHandle;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@CapabilityDescription("Allows for MarkLogic REST extensions to be called with results being sent to new FlowFiles. " +
    "This is intended to replace ExtensionCallMarkLogic, which is now deprecated.")
public class CallRestExtensionMarkLogic extends ExtensionCallMarkLogic {

    protected static final Relationship RESULTS = new Relationship.Builder().name("results")
        .description("Each item returned by the extension call to MarkLogic results in a new " +
            "FlowFile being sent to this relationship. Each FlowFile retains all of the " +
            "attributes from the incoming FlowFile if one exists.").build();

    protected static final Relationship ORIGINAL = new Relationship.Builder().name("original")
        .description("The original FlowFile - either the incoming one or a new one if 'Requires Input' is false - " +
            "will be sent here after the extension call to MarkLogic is completed.").build();


    /**
     * Overrides the relationships set up by the parent class.
     *
     * @param context in which to perform initialization
     */
    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);
        Set<Relationship> set = new HashSet<>();
        set.add(ORIGINAL);
        set.add(RESULTS);
        set.add(FAILURE);
        relationships = Collections.unmodifiableSet(set);
    }

    /**
     * @param results
     * @param session
     * @param originalFlowFile
     */
    protected void handleExtensionCallResults(ResourceServices.ServiceResultIterator results, ProcessSession session, FlowFile originalFlowFile) {
        if (results == null) {
            transferAndCommit(session, originalFlowFile, ORIGINAL);
            return;
        }
        try {
            while (results.hasNext()) {
                FlowFile resultFlowFile = session.create(originalFlowFile);
                session.write(resultFlowFile, out -> out.write(results.next().getContent(new BytesHandle()).get()));
                session.transfer(resultFlowFile, RESULTS);
            }
        } finally {
            results.close();
            transferAndCommit(session, originalFlowFile, ORIGINAL);
        }
    }
}
