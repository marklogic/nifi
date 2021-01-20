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
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import com.marklogic.client.datamovement.ApplyTransformListener;
import com.marklogic.client.datamovement.ApplyTransformListener.ApplyResult;
import com.marklogic.client.datamovement.QueryBatchListener;

@Tags({ "MarkLogic", "Transform", "ApplyTransform", "Update" })
@InputRequirement(Requirement.INPUT_ALLOWED)
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@CapabilityDescription("Creates FlowFiles from batches of documents, matching the given criteria,"
        + " transformed from a MarkLogic server using the MarkLogic Data Movement SDK (DMSDK)")
@DynamicProperty(name = "Server transform parameter name", value = "Value of the server transform parameter",
description = "Adds server transform parameters to be passed to the server transform specified. "
+ "Server transform parameter name should start with the string 'trans:'.",
expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The filename is set to the uri of the document deleted from MarkLogic") })
@Stateful(description = "Can keep state of a range index value to restrict future queries.", scopes = { Scope.CLUSTER })
public class ApplyTransformMarkLogic extends QueryMarkLogic {
    public static final PropertyDescriptor APPLY_RESULT_TYPE = new PropertyDescriptor.Builder()
            .name("Apply Result Type").displayName("Apply Result Type").defaultValue(ApplyResultTypes.REPLACE.getValue())
            .description("Whether to REPLACE each document with the result of the transform, or run the transform with each document as input, but IGNORE the result.").required(true)
            .allowableValues(ApplyResultTypes.allValues)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor TRANSFORM = new PropertyDescriptor.Builder()
            .name("Server Transform")
            .displayName("Server Transform")
            .description("The name of REST server transform to apply to every document")
            .addValidator(Validator.VALID)
            .required(true)
            .build();

    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(DATABASE_CLIENT_SERVICE);
        list.add(BATCH_SIZE);
        list.add(THREAD_COUNT);
        list.add(CONSISTENT_SNAPSHOT);
        list.add(QUERY);
        list.add(QUERY_TYPE);
        list.add(APPLY_RESULT_TYPE);
        list.add(TRANSFORM);
        list.add(STATE_INDEX);
        list.add(STATE_INDEX_TYPE);
        properties = Collections.unmodifiableList(list);
        Set<Relationship> set = new HashSet<>();
        set.add(SUCCESS);
        set.add(FAILURE);
        relationships = Collections.unmodifiableSet(set);
    }

    protected QueryBatchListener buildQueryBatchListener(final ProcessContext context, final ProcessSession session, final boolean consistentSnapshot) {
        ApplyTransformListener applyTransform = new ApplyTransformListener()
            .withApplyResult(
                ApplyResultTypes.INGORE_STR.equals(context.getProperty(APPLY_RESULT_TYPE).getValue()) ? ApplyResult.IGNORE : ApplyResult.REPLACE
            )
            .withTransform(this.buildServerTransform(context))
            .onSuccess((batch) -> {
                synchronized(session) {
                    for (String uri: batch.getItems()) {
                        final FlowFile flowFile = session.create();
                        session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), uri);
                        session.transfer(flowFile, SUCCESS);
                    }
                    session.commit();
                }
            })
            .onFailure((batch, throwable) -> {
                getLogger().error("Error processing transform", throwable);
                synchronized(session) {
                    for (String uri: batch.getItems()) {
                        final FlowFile flowFile = session.create();
                        session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), uri);
                        session.transfer(flowFile, FAILURE);
                    }
                    session.commit();
                }
                context.yield();
            });
        return applyTransform;
    }

    public static class ApplyResultTypes extends AllowableValuesSet {
        public static final String INGORE_STR = "Ignore";
        public static final AllowableValue IGNORE = new AllowableValue(INGORE_STR, INGORE_STR,
                "Run the transform on each document, but ignore the value returned by the transform because the transform " +
                "will do any necessary database modifications or other processing. For example, a transform might call out " +
                "to an external REST service or perhaps write multiple additional documents.");
        public static final String REPLACE_STR = "Replace";
        public static final AllowableValue REPLACE = new AllowableValue(REPLACE_STR, REPLACE_STR,
                "Overwrites documents with the value returned by the transform, just like REST write transforms. This is the default behavior.");

        public static final AllowableValue[] allValues = new AllowableValue[] { IGNORE, REPLACE };

    }

}
