package org.apache.nifi.marklogic.processor;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RowManager;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.springframework.util.FileCopyUtils;

import java.io.InputStream;
import java.util.*;

@Tags({"MarkLogic", "Query", "Read", "Rows"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Executes a serialized Optic query plan and writes the returned rows to a FlowFile")
public class QueryRowsMarkLogic extends AbstractMarkLogicProcessor {

    public static final PropertyDescriptor PLAN = new PropertyDescriptor.Builder()
        .name("Plan")
        .displayName("Plan")
        .description("A serialized Optic query plan; see https://docs.marklogic.com/guide/app-dev/OpticAPI#id_11208 for information on how to generate a plan")
        .required(true)
        .addValidator(Validator.VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    public static final PropertyDescriptor MIMETYPE = new PropertyDescriptor.Builder()
        .name("MIME Type")
        .displayName("MIME Type")
        .description("The MIME type to use when returning the rows")
        .defaultValue("text/csv")
        .required(true)
        .addValidator(Validator.VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    protected static final Relationship FAILURE = new Relationship.Builder().name("failure")
        .description("If any error occurs while querying for rows, the incoming FlowFile, or a new one if an incoming one " +
            "does not exist, will be sent here").build();

    protected static final Relationship SUCCESS = new Relationship.Builder().name("success")
        .description("A FlowFile is routed here with its content set to the list of matching rows").build();

    protected static final Relationship ORIGINAL = new Relationship.Builder().name("original")
        .autoTerminateDefault(true)
        .description("The incoming FlowFile, or a new one if an incoming one does not exist, is sent here").build();

    @Override
    public void init(ProcessorInitializationContext context) {
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(DATABASE_CLIENT_SERVICE);
        list.add(PLAN);
        list.add(MIMETYPE);
        properties = Collections.unmodifiableList(list);

        Set<Relationship> set = new HashSet<>();
        set.add(FAILURE);
        set.add(SUCCESS);
        set.add(ORIGINAL);
        relationships = Collections.unmodifiableSet(set);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        FlowFile incomingFlowFile = session.get();
        if (incomingFlowFile == null) {
            incomingFlowFile = session.create();
        }

        try {
            final String jsonPlan = determineJsonPlan(context, incomingFlowFile);
            final String mimeType = determineMimeType(context, incomingFlowFile);

            session.putAttribute(incomingFlowFile, "marklogic-optic-plan", jsonPlan);

            final DatabaseClient client = getDatabaseClient(context);
            final RowManager rowManager = client.newRowManager();
            PlanBuilder.Plan plan = rowManager.newRawPlanDefinition(new StringHandle(jsonPlan));
            try (InputStreamHandle handle = new InputStreamHandle()){
                try (InputStream inputStream = rowManager.resultDoc(plan, handle.withMimetype(mimeType))
                        .get()) {
                    if (inputStream != null) {
                        FlowFile resultFlowFile = session.write(
                            session.create(incomingFlowFile),
                            out -> FileCopyUtils.copy(inputStream, out));
                        session.transfer(resultFlowFile, SUCCESS);
                    }
                    transferAndCommit(session, incomingFlowFile, ORIGINAL);
                }
            }

        } catch (final Throwable t) {
            logErrorAndTransfer(t, incomingFlowFile, session, FAILURE);
        }
    }

    protected String determineJsonPlan(ProcessContext context, FlowFile flowFile) {
        return context.getProperty(PLAN).evaluateAttributeExpressions(flowFile).getValue();
    }

    protected String determineMimeType(ProcessContext context, FlowFile flowFile) {
        return context.getProperty(MIMETYPE).evaluateAttributeExpressions(flowFile).getValue();
    }

}
