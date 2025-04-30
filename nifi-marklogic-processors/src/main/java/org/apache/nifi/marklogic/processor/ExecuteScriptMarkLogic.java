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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.EvalResult;
import com.marklogic.client.eval.ServerEvaluationCall;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;

@Tags({"MarkLogic", "database", "XQuery", "JavaScript", "module", "server-side"})
@CapabilityDescription("Executes server-side code in MarkLogic, either in JavaScript or XQuery. "
    + "Code can be given in a Script Body property or can be invoked as a path to a module installed on the server. " +
    "Requires a MarkLogic user with the privileges listed at https://docs.marklogic.com/REST/POST/v1/eval .")
@DynamicProperty(
    name = "Any name",
    value = "Any value",
    expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    description = "Every dynamic property on an instance of this processor will be added as a variable when executing the script"
)
public class ExecuteScriptMarkLogic extends AbstractMarkLogicProcessor {

    public static final String MARKLOGIC_RESULT = "marklogic.result";
    public static final String MARKLOGIC_RESULTS_COUNT = "marklogic.results.count";

    protected static Validator PATH_SCRIPT_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            String scriptBody = context.getProperty(SCRIPT_BODY).getValue();
            String modulePath = context.getProperty(MODULE_PATH).getValue();
            if (StringUtils.isEmpty(scriptBody) == StringUtils.isEmpty(modulePath)) {
                return new ValidationResult.Builder().valid(false)
                    .explanation("Exactly one of Module Path or Script Body must be set").build();
            } else {
                return new ValidationResult.Builder().valid(true).build();
            }
        }
    };

    // ---------- Execution Type property ----------

    public static final String STR_XQUERY = "XQuery";
    public static final String STR_JAVASCRIPT = "JavaScript";
    public static final String STR_MODULE_PATH = "Module Path";

    public static final AllowableValue AV_XQUERY = new AllowableValue(STR_XQUERY, STR_XQUERY,
        "Execute XQuery supplied in the Script Body property");

    public static final AllowableValue AV_JAVASCRIPT = new AllowableValue(STR_JAVASCRIPT, STR_JAVASCRIPT,
        "Execute JavaScript supplied in the Script Body property");

    public static final AllowableValue AV_MODULE_PATH = new AllowableValue(STR_MODULE_PATH, STR_MODULE_PATH,
        "Execute the module specified in the Module Path property");

    public static final PropertyDescriptor EXECUTION_TYPE = new PropertyDescriptor.Builder().name("Execution Type")
        .displayName("Execution Type")
        .description("What will be executed: ad-hoc XQuery or JavaScript, or a path to a module on the server")
        .allowableValues(AV_XQUERY, AV_JAVASCRIPT, AV_MODULE_PATH).required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    // ---------- Results Destination property ----------

    public static final String STR_CONTENT = "Content";
    public static final String STR_ATTRIBUTE = "Attribute";
    public static final String STR_FROMJSON = "Attributes from JSON Properties";

    public static final AllowableValue AV_CONTENT = new AllowableValue(STR_CONTENT, STR_CONTENT,
        "Write the MarkLogic result to the FlowFile content");

    public static final AllowableValue AV_ATTRIBUTE = new AllowableValue(STR_ATTRIBUTE, STR_ATTRIBUTE,
        "Write the MarkLogic result to the marklogic.result attribute");

    public static final AllowableValue AV_FROMJSON = new AllowableValue(STR_FROMJSON, STR_FROMJSON,
        "Parse a MarkLogic JSON result into attributes with the same names as the top-level JSON properties, where the values are simple types, not objects or arrays");

    public static final PropertyDescriptor RESULTS_DESTINATION = new PropertyDescriptor.Builder()
        .name("Results Destination").displayName("Results Destination")
        .description("Where each result will be written in the FlowFile; "
            + "if 'Attribute', the result will be written to the 'marklogic.result' attribute")
        .allowableValues(AV_CONTENT, AV_ATTRIBUTE, AV_FROMJSON).required(true).defaultValue(STR_CONTENT)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        // Validators don't execute on non-required properties. We will attach the
        // Script Body / Module Path validator here as a 2nd validator because this is
        // required.
        .addValidator(PATH_SCRIPT_VALIDATOR).build();

    public static final PropertyDescriptor SKIP_FIRST = new PropertyDescriptor.Builder().name("Skip First Result")
        .displayName("Skip First Result")
        .description("If true, first result is not sent to results relationship or "
            + "last result relationship, but is sent to the first result relationship")
        .allowableValues("true", "false").required(true).defaultValue("false")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    // ---------------------------

    public static final PropertyDescriptor SCRIPT_BODY = new PropertyDescriptor.Builder().name("Script Body")
        .displayName("Script Body")
        .description("Body of script to execute; only one of Module Path or Script Body may be used")
        .required(false).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(Validator.VALID).build();

    public static final PropertyDescriptor MODULE_PATH = new PropertyDescriptor.Builder().name("Module Path")
        .displayName("Module Path")
        .description("Path of module to execute; only one of Module Path or Script Body may be used")
        .required(false).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(Validator.VALID).build();

    public static final PropertyDescriptor CONTENT_VARIABLE = new PropertyDescriptor.Builder().name("Content Variable")
        .displayName("Content Variable")
        .description(
            "The name of the external variable in the script that will be populated with the content of the incoming FlowFile")
        .required(false).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(Validator.VALID).build();

    // ---------- RELATIONSHIPS ----------

    protected static final Relationship RESULTS = new Relationship.Builder().name("results")
        .description("Receives a FlowFile for each result returned by the executed script; will not receive a " +
            "FlowFile for the first result if 'Skip First Result' is true").build();

    protected static final Relationship FIRST_RESULT = new Relationship.Builder().name("first result")
        .description("Receives a FlowFile for the first result returned by the executed script").build();

    protected static final Relationship LAST_RESULT = new Relationship.Builder().name("last result")
        .description("Receives a FlowFile for the last result returned by the executed script if it returns at " +
            "least two results").build();

    protected static final Relationship ORIGINAL = new Relationship.Builder().name("original")
        .description("Receives the original FlowFile that this processor received").build();

    protected static final Relationship FAILURE = new Relationship.Builder().name("failure")
        .description("Receives the original FlowFile if the call to MarkLogic fails for any reason").build();


    private static Charset UTF8 = StandardCharsets.UTF_8;

    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DATABASE_CLIENT_SERVICE);
        descriptors.add(EXECUTION_TYPE);
        descriptors.add(SCRIPT_BODY);
        descriptors.add(MODULE_PATH);
        descriptors.add(CONTENT_VARIABLE);
        descriptors.add(RESULTS_DESTINATION);
        descriptors.add(SKIP_FIRST);
        this.properties = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(RESULTS);
        relationships.add(FIRST_RESULT);
        relationships.add(LAST_RESULT);
        relationships.add(ORIGINAL);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory)
        throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        FlowFile incomingFlowFile = null;
        try {
            incomingFlowFile = session.get();
            if (incomingFlowFile == null) {
                incomingFlowFile = session.create();
            }

            final String resultsDest = context.getProperty(RESULTS_DESTINATION).getValue();
            final String contentVariable = context.getProperty(CONTENT_VARIABLE).evaluateAttributeExpressions(incomingFlowFile).getValue();
            final boolean skipFirst = context.getProperty(SKIP_FIRST).getValue().equals("true");

            ServerEvaluationCall call = buildCall(context, session, incomingFlowFile);

            // write the content to the contentVariable external variable, if supplied
            if (contentVariable != null && contentVariable.length() > 0) {
                final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                session.exportTo(incomingFlowFile, bytes);
                final String content = bytes.toString(StandardCharsets.UTF_8);

                call.addVariable(contentVariable, content);
            }

            // iterate over the dynamic properties and set as variables to send to the
            // script,
            for (PropertyDescriptor entry : context.getProperties().keySet()) {
                if (entry.isDynamic()) {
                    String name = entry.getName();
                    String value = context.getProperty(name).evaluateAttributeExpressions(incomingFlowFile).getValue();
                    call.addVariable(name, value);
                }
            }

            int count = 0;
            String last = null;

            for (EvalResult result : call.eval()) {
                count++;
                final String resultStr = result.getString();
                last = resultStr;
                if (count == 1) {
                    FlowFile firstFF = createFlowFileWithAttributes(session, incomingFlowFile.getAttributes());
                    resultToFlowFile(session, resultStr, firstFF, resultsDest);
                    session.transfer(firstFF, FIRST_RESULT);
                }
                if (count > 1 || !skipFirst) {
                    FlowFile resultFF = createFlowFileWithAttributes(session, incomingFlowFile.getAttributes());
                    resultToFlowFile(session, resultStr, resultFF, resultsDest);
                    session.transfer(resultFF, RESULTS);
                }
            }

            incomingFlowFile = session.putAttribute(incomingFlowFile, MARKLOGIC_RESULTS_COUNT, Integer.toString(count));
            session.transfer(incomingFlowFile, ORIGINAL);

            if (last != null && (count > 1 || !skipFirst)) {
                FlowFile lastFF = createFlowFileWithAttributes(session, incomingFlowFile.getAttributes());
                resultToFlowFile(session, last, lastFF, resultsDest);
                session.transfer(lastFF, LAST_RESULT);
            }

            session.commitAsync();
        } catch (final Throwable t) {
            logErrorAndTransfer(t, incomingFlowFile, session, FAILURE);
        }
    }

    private ServerEvaluationCall buildCall(ProcessContext context, ProcessSession session, FlowFile originalFlowFile) {
        DatabaseClient client = getDatabaseClient(context);
        Objects.requireNonNull(context.getProperty(EXECUTION_TYPE), "EXECUTION_TYPE property should not be null");
        final String executionType = context.getProperty(EXECUTION_TYPE).getValue();
        ServerEvaluationCall call = client.newServerEval();

        if (STR_MODULE_PATH.equals(executionType)) {
            Objects.requireNonNull(context.getProperty(MODULE_PATH), "MODULE_PATH property should not be null");
            String modulePath = context.getProperty(MODULE_PATH).evaluateAttributeExpressions(originalFlowFile).getValue();
            session.putAttribute(originalFlowFile, "marklogic-module-path", modulePath);
            return call.modulePath(modulePath);
        }

        Objects.requireNonNull(context.getProperty(SCRIPT_BODY), "SCRIPT_BODY property should not be null");
        final String scriptBody = context.getProperty(SCRIPT_BODY).evaluateAttributeExpressions(originalFlowFile).getValue();
        session.putAttribute(originalFlowFile, "marklogic-script-body", scriptBody);
        return STR_JAVASCRIPT.equals(executionType) ? call.javascript(scriptBody) : call.xquery(scriptBody);
    }

    private void resultToFlowFile(ProcessSession session, String resultStr, FlowFile flowFile, String resultsDest) {
        synchronized (session) {
            if (resultsDest.equals("Content")) {
                // write the query result to the FlowFile content
                session.write(flowFile, (in, out) -> out.write(resultStr.getBytes(UTF8)));
            } else if (resultsDest.equals("Attribute")) {
                session.putAttribute(flowFile, MARKLOGIC_RESULT, resultStr);
            } else {
                JsonElement jelement = JsonParser.parseString(resultStr);
                JsonObject obj = jelement.getAsJsonObject();
                for (Entry<String, JsonElement> entry : obj.entrySet()) {
                    String property = entry.getKey();
                    String value;
                    JsonElement propertyValue = entry.getValue();
                    if (propertyValue instanceof JsonObject) {
                        value = "[object]";
                    } else if (propertyValue instanceof JsonArray) {
                        value = "[array]";
                    } else if (propertyValue == null || propertyValue.isJsonNull()) {
                        value = "";
                    } else {
                        value = entry.toString();
                    }

                    if (value == null) {
                        value = "";
                    }
                    session.putAttribute(flowFile, property, value);
                }
            }
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        PropertyDescriptor.Builder propertyBuilder = new PropertyDescriptor.Builder().name(propertyDescriptorName)
            .required(false).addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).dynamic(true);

        return propertyBuilder.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    }
}
