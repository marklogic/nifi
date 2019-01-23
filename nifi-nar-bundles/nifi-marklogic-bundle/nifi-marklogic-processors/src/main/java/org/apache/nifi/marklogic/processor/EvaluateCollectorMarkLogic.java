package org.apache.nifi.marklogic.processor;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

/**
 * Evaluates the collector module in a Data Hub Framework harmonize flow. The results are written as new FlowFiles, with
 * each FlowFile containing a number of identifiers matching the value of the "batch size" property.
 * <p>
 * This processor allows input but does not require it. This allows for it to be the start of a flow - e.g. a flow that
 * runs every hour and collects identifiers to harmonize - or to run after another processor - e.g. a flow that may
 * first ingest data and then kick off a harmonization process.
 */
@Tags({"MarkLogic", "REST", "Data Hub Framework"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Evaluates a MarkLogic Data Hub Framework collector and creates a FlowFile for each batch of identifiers")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "In order to set fragment.count on each FlowFile, all of the " +
	"FlowFiles must be temporarily stored in memory so that the count can be determined.")
@WritesAttributes({
	@WritesAttribute(attribute = "fragment.identifier", description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute."),
	@WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile."),
	@WritesAttribute(attribute = "fragment.count", description = "The number of new FlowFiles created from the identifiers returned by the collector. Only set if 'Set Fragment Count' is true."),
	@WritesAttribute(attribute = "dhf.collector.identifiers.total", description = "The count of identifiers returned by the collector. Only set if 'Set Fragment Count' is true.")
})
public class EvaluateCollectorMarkLogic extends AbstractMarkLogicProcessor {

	public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
	public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
	public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();

	public static final String SCRIPT_TYPE_XQUERY = "XQuery";
	public static final String SCRIPT_TYPE_JAVASCRIPT = "JavaScript";

	public static final PropertyDescriptor ENTITY_NAME = new PropertyDescriptor.Builder()
		.name("Entity Name")
		.displayName("Entity Name")
		.description("The name of the entity associated with the collector you wish to run. If set, Flow Name should be set as well.")
		.required(false)
		.addValidator(Validator.VALID)
		.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
		.build();

	public static final PropertyDescriptor FLOW_NAME = new PropertyDescriptor.Builder()
		.name("Flow Name")
		.displayName("Flow Name")
		.description("The name of the harmonize flow associated with the collector you wish to run. If set, Entity Name should be set as well.")
		.required(false)
		.addValidator(Validator.VALID)
		.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
		.build();

	public static final PropertyDescriptor OPTIONS = new PropertyDescriptor.Builder()
		.name("Options")
		.displayName("Options")
		.description("An optional JSON object defining options that are passed to the collector module.")
		.required(false)
		.addValidator(Validator.VALID)
		.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
		.build();

	public static final PropertyDescriptor SCRIPT_BODY = new PropertyDescriptor.Builder()
		.name("Script Body")
		.displayName("Script Body")
		.description("A JavaScript or XQuery script to evaluate that runs a collector. When using this, Entity Name and Flow Name will be ignored.")
		.required(false)
		.addValidator(Validator.VALID)
		.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
		.build();

	public static final PropertyDescriptor SCRIPT_TYPE = new PropertyDescriptor.Builder()
		.name("Script Type")
		.displayName("Script Type")
		.description("If Script Body is set, set this to define whether the script is JavaScript or XQuery.")
		.required(false)
		.allowableValues(SCRIPT_TYPE_JAVASCRIPT, SCRIPT_TYPE_XQUERY)
		.addValidator(Validator.VALID)
		.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
		.build();

	public static final PropertyDescriptor DELIMITER = new PropertyDescriptor.Builder()
		.name("Delimiter")
		.displayName("Delimiter")
		.description("Used to delimiter the identifiers in each FlowFile routed to the 'batches' relationship; " +
			"the identifiers are stored in a FlowFile attribute named 'collectorIdentifiers'")
		.required(true)
		.defaultValue(",")
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
		.build();

	public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
		.name("Batch Size")
		.displayName("Batch Size")
		.description("The number of identifiers to include in each FlowFile routed to the 'batches' relationship")
		.required(true)
		.defaultValue("100")
		.addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
		.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
		.build();

	public static final PropertyDescriptor IDENTIFIERS_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
		.name("Identifiers Attribute Name")
		.displayName("Identifiers Attribute Name")
		.description("The name of the FlowFile attribute that the batch of delimited identifiers is stored in")
		.required(true)
		.defaultValue("dhf.collector.identifiers")
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
		.build();

	public static final PropertyDescriptor SET_FRAGMENT_COUNT = new PropertyDescriptor.Builder()
		.name("Set Fragment Count")
		.displayName("Set Fragment Count")
		.description("If set to true, then each FlowFile routed to 'batches' will have a fragment.count attribute set. " +
			"This allows for a processor like MergeContent to know when all of the identifiers have been processed (e.g. via ExtensionCallMarkLogic). " +
			"Note that, similar to SplitText, this can cause memory issues if the collector returns a large number of identifiers. If you do not need this capability, " +
			"be sure that this is set to false.")
		.required(true)
		.allowableValues("true", "false")
		.defaultValue("false")
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
		.build();

	protected static final Relationship BATCHES = new Relationship.Builder().name("batches")
		.description("FlowFiles created from each batch of collected identifiers are routed here")
		.build();

	protected static final Relationship FAILURE = new Relationship.Builder().name("failure")
		.description("FlowFiles that were not successfully processed are routed here").build();

	protected static final Relationship ORIGINAL = new Relationship.Builder().name("original")
		.description("The original FlowFile received by this processor is routed here").build();

	@Override
	public void init(ProcessorInitializationContext context) {
		List<PropertyDescriptor> list = new ArrayList<>();
		list.add(DATABASE_CLIENT_SERVICE);
		list.add(ENTITY_NAME);
		list.add(FLOW_NAME);
		list.add(OPTIONS);
		list.add(SCRIPT_BODY);
		list.add(SCRIPT_TYPE);
		list.add(DELIMITER);
		list.add(BATCH_SIZE);
		list.add(IDENTIFIERS_ATTRIBUTE_NAME);
		list.add(SET_FRAGMENT_COUNT);
		properties = Collections.unmodifiableList(list);

		Set<Relationship> set = new HashSet<>();
		set.add(BATCHES);
		set.add(FAILURE);
		set.add(ORIGINAL);
		relationships = Collections.unmodifiableSet(set);
	}

	/**
	 * This processor does not require an input FlowFile. If one does not exist, and there's no incoming connection,
	 * If a FlowFile exists, builds a script to evaluate a DHF collector module. Evaluates the script and then creates
	 * a new FlowFile for each batch of identifiers in the response, where the size and structure of a batch is
	 * determined by the Batch Size and Delimiter properties. Each FlowFile has the batch stored as an attribute with
	 * a name determined by the Identifiers Attribute Name property.
	 *
	 * @param context
	 * @param sessionFactory
	 * @throws ProcessException
	 */
	@Override
	public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
		final ProcessSession session = sessionFactory.createSession();

		/**
		 * At least in NiFi 1.8.0, if this processor has an incoming connection, then onTrigger won't be invoked unless
		 * a FlowFile exists. So if onTrigger is invoked, either a FlowFile exists, or there's no incoming connection
		 * and the user does not require any input, and thus a new FlowFile should be created.
		 */
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			flowFile = session.create();
		}

		try {
			final String script = buildScriptToEvaluate(context, flowFile);
			getLogger().info("Evaluating script: " + script);

			EvalResultIterator iterator = evaluateScript(context, flowFile, script);
			try {
				createFlowFilesForBatchesOfIdentifiers(context, session, flowFile, iterator);
				transferAndCommit(session, flowFile, ORIGINAL);
			} finally {
				iterator.close();
			}
		} catch (Exception ex) {
			transferAndCommit(session, flowFile, FAILURE);
		}
	}

	/**
	 * TODO Add unit tests for this.
	 *
	 * @param context
	 * @return
	 */
	protected String buildScriptToEvaluate(ProcessContext context, FlowFile flowFile) {
		final String scriptBody = context.getProperty(SCRIPT_BODY).evaluateAttributeExpressions(flowFile).getValue();
		if (scriptBody != null && scriptBody.trim().length() > 0) {
			return scriptBody;
		}

		final String entityName = context.getProperty(ENTITY_NAME).evaluateAttributeExpressions(flowFile).getValue();
		final String flowName = context.getProperty(FLOW_NAME).evaluateAttributeExpressions(flowFile).getValue();
		return String.format("var options; const collector = require('/entities/%s/harmonize/%s/collector.sjs'); collector.collect(options)",
			entityName, flowName);
	}

	protected EvalResultIterator evaluateScript(ProcessContext context, FlowFile flowFile, String script) {
		final String scriptType = context.getProperty(SCRIPT_TYPE).evaluateAttributeExpressions(flowFile).getValue();
		final String options = context.getProperty(OPTIONS).evaluateAttributeExpressions(flowFile).getValue();

		DatabaseClient client = getDatabaseClient(context);
		ServerEvaluationCall call = client.newServerEval();
		call = SCRIPT_TYPE_XQUERY.equals(scriptType) ? call.xquery(script) : call.javascript(script);

		if (options != null && options.trim().length() > 0) {
			getLogger().info("Collector options: " + options);
			call.addVariable("options", new StringHandle(options).withFormat(Format.JSON));
		}

		return call.eval();
	}

	/**
	 * Similar to SplitText, this needs to generate all of the new FlowFile objects first, and then set fragment.count
	 * on them.
	 * <p>
	 * TODO Should be able to add unit tests for this, can mock an EvalResultIterator.
	 *
	 * @param context
	 * @param session
	 * @param flowFile
	 * @param iterator
	 */
	protected void createFlowFilesForBatchesOfIdentifiers(ProcessContext context, ProcessSession session, FlowFile flowFile, EvalResultIterator iterator) {
		final String delimiter = context.getProperty(DELIMITER).evaluateAttributeExpressions(flowFile).getValue();
		final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions(flowFile).asInteger();
		final String identifiersAttributeName = context.getProperty(IDENTIFIERS_ATTRIBUTE_NAME).evaluateAttributeExpressions(flowFile).getValue();
		final boolean setFragmentCount = context.getProperty(SET_FRAGMENT_COUNT).evaluateAttributeExpressions(flowFile).asBoolean();

		String delimitedIdentifiers = null;
		int totalIdentifierCount = 0;
		int identifierCount = 0;
		int index = 1;
		final String newFragmentId = UUID.randomUUID().toString();

		List<FlowFile> newFlowFiles = new ArrayList<>();

		while (iterator.hasNext()) {
			final String identifier = iterator.next().getString();
			if (delimitedIdentifiers != null) {
				delimitedIdentifiers = delimitedIdentifiers + delimiter + identifier;
			} else {
				delimitedIdentifiers = identifier;
			}
			identifierCount++;
			totalIdentifierCount++;
			if (identifierCount >= batchSize) {
				FlowFile newFlowFile = buildNewFlowFile(session, flowFile, index, identifiersAttributeName, delimitedIdentifiers, newFragmentId);
				if (setFragmentCount) {
					newFlowFiles.add(newFlowFile);
				} else {
					session.transfer(newFlowFile, BATCHES);
				}
				index++;
				identifierCount = 0;
				delimitedIdentifiers = null;
			}
		}
		if (delimitedIdentifiers != null) {
			FlowFile newFlowFile = buildNewFlowFile(session, flowFile, index, identifiersAttributeName, delimitedIdentifiers, newFragmentId);
			if (setFragmentCount) {
				newFlowFiles.add(newFlowFile);
			} else {
				session.transfer(newFlowFile, BATCHES);
			}
		}

		if (setFragmentCount) {
			final String fragmentCount = String.valueOf(newFlowFiles.size());
			final String collectorIdentifiersCount = String.valueOf(totalIdentifierCount);
			final ListIterator<FlowFile> flowFileItr = newFlowFiles.listIterator();
			while (flowFileItr.hasNext()) {
				FlowFile splitFlowFile = flowFileItr.next();
				Map<String, String> attributes = new HashMap<>();
				attributes.put(FRAGMENT_COUNT, fragmentCount);
				attributes.put("dhf.collector.identifiers.total", collectorIdentifiersCount);
				final FlowFile updated = session.putAllAttributes(splitFlowFile, attributes);
				flowFileItr.set(updated);
			}
			session.transfer(newFlowFiles, BATCHES);
			getLogger().info("Finished creating {} FlowFiles for {} identifiers", new Object[]{fragmentCount, totalIdentifierCount});
		} else {
			getLogger().info("Finished creating FlowFiles for {} identifiers", new Object[]{totalIdentifierCount});
		}
	}

	/**
	 * TODO Add unit tests for this.
	 *
	 * @param session
	 * @param flowFile
	 * @param index
	 * @param identifiersAttributeName
	 * @param delimitedIdentifiers
	 * @param fragmentId
	 * @return
	 */
	protected FlowFile buildNewFlowFile(ProcessSession session, FlowFile flowFile, int index,
	                                    String identifiersAttributeName, String delimitedIdentifiers, String fragmentId) {
		FlowFile newFlowFile = session.create(flowFile);
		Map<String, String> attributes = new HashMap<>();
		attributes.put(FRAGMENT_ID, fragmentId);
		attributes.put(FRAGMENT_INDEX, String.valueOf(index));
		attributes.put(identifiersAttributeName, delimitedIdentifiers);
		return session.putAllAttributes(newFlowFile, attributes);
	}

	/**
	 * TODO Can eventually move this to parent class for reuse across processors.
	 *
	 * @param session
	 * @param flowFile
	 * @param relationship
	 */
	protected void transferAndCommit(ProcessSession session, FlowFile flowFile, Relationship relationship) {
		synchronized (session) {
			session.transfer(flowFile, relationship);
			session.commit();
		}
	}
}
