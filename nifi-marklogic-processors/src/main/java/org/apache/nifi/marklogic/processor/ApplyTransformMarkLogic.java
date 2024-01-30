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

import com.marklogic.client.datamovement.ApplyTransformListener;
import com.marklogic.client.datamovement.ApplyTransformListener.ApplyResult;
import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.datamovement.QueryBatchListener;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
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

import java.util.*;

@Tags({"MarkLogic", "Transform", "ApplyTransform", "Update"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("Creates FlowFiles from batches of documents, matching the given criteria,"
    + " with each document transformed via a MarkLogic REST server transform. Requires a MarkLogic user " +
    "with the 'rest-reader' privilege to read documents and the 'rest-writer' privilege to update transformed documents.")
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
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY
    )
})
@Stateful(description = "Can keep state of a range index value to restrict future queries.", scopes = {Scope.CLUSTER})
public class ApplyTransformMarkLogic extends QueryMarkLogic {
    public static final PropertyDescriptor APPLY_RESULT_TYPE = new PropertyDescriptor.Builder()
        .name("Apply Result Type").displayName("Apply Result Type").defaultValue(ApplyResultTypes.REPLACE.getValue())
        .description("Whether to replace each document with the result of the transform, or run the transform with each document as input, but ignore the result").required(true)
        .allowableValues(ApplyResultTypes.allValues)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .build();
    public static final PropertyDescriptor TRANSFORM = new PropertyDescriptor.Builder()
        .name("Server Transform")
        .displayName("Server Transform")
        .description("The name of a REST server transform to apply to each document; " +
            "see https://docs.marklogic.com/guide/rest-dev/transforms for more information on REST transforms.")
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
        set.add(ORIGINAL);
        set.add(SUCCESS);
        set.add(FAILURE);
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
    protected QueryBatchListener buildQueryBatchListener(final ProcessContext context, final ProcessSession session, Map<String, String> incomingAttributes) {
        return new ApplyTransformListener()
            .withApplyResult(
                ApplyResultTypes.INGORE_STR.equals(context.getProperty(APPLY_RESULT_TYPE).getValue()) ? ApplyResult.IGNORE : ApplyResult.REPLACE
            )
            .withTransform(this.buildServerTransform(context))
            .onSuccess(batch -> transferBatch(session, incomingAttributes, batch, SUCCESS, null))
            .onFailure((batch, throwable) -> {
                getLogger().error("Unable to apply transform to batch of URIs; cause: " + throwable.getMessage());
                transferBatch(session, incomingAttributes, batch, FAILURE, throwable);
            });
    }

    public static class ApplyResultTypes {
        public static final String INGORE_STR = "Ignore";
        public static final AllowableValue IGNORE = new AllowableValue(INGORE_STR, INGORE_STR,
            "Run the transform on each document, but ignore the value returned by the transform because the transform " +
                "will do any necessary database modifications or other processing. For example, a transform might call out " +
                "to an external REST service or perhaps write multiple additional documents.");
        public static final String REPLACE_STR = "Replace";
        public static final AllowableValue REPLACE = new AllowableValue(REPLACE_STR, REPLACE_STR,
            "Replace a document with the output produced by running the transform on the document");

        public static final AllowableValue[] allValues = new AllowableValue[]{IGNORE, REPLACE};

    }

}
