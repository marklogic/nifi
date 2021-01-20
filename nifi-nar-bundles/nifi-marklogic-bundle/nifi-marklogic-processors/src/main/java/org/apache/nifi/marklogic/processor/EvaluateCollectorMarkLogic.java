package org.apache.nifi.marklogic.processor;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
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

	public static final String IDENTIFIERS_ATTRIBUTE = "dhf.collector.identifiers";
	public static final String IDENTIFIERS_TOTAL_ATTRIBUTE = "dhf.collector.identifiers.total";
	
	public static final PropertyDescriptor DHF_VERSION = new PropertyDescriptor.Builder()
			.name("DHF Version")
			.displayName("DHF Version (Major)")
			.description("DHF Version (Major)")
			.required(false)
			.allowableValues("4", "5")
			.defaultValue("4")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();	
	
	public static final PropertyDescriptor STEP = new PropertyDescriptor.Builder()
			.name("Step No")
			.displayName("Step No")
			.description("The Step Number property describes the DHF5 step for which you wish to run the collector. This is only applicable when DHF Version is 5")
			.required(false)
			.addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.build();
	
	public static final PropertyDescriptor ENTITY_NAME = new PropertyDescriptor.Builder()
		.name("Entity Name")
		.displayName("Entity Name")
		.description("The name of the entity associated with the collector you wish to run. If set, Flow Name should be set as well. " +
			"Assumes that the collector module is written in SJS. If this is not the case, use SCRIPT_BODY instead.")
		.required(false)
		.addValidator(Validator.VALID)
		.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
		.build();

	public static final PropertyDescriptor FLOW_NAME = new PropertyDescriptor.Builder()
		.name("Flow Name")
		.displayName("Flow Name")
		.description("The name of the harmonize flow associated with the collector you wish to run. If set, Entity Name should be set as well." +
			"Assumes that the collector module is written in SJS. If this is not the case, use SCRIPT_BODY instead.")
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
		.description("A JavaScript or XQuery script to evaluate that runs a collector. When using this, Entity Name and Flow Name will be ignored. Use this " +
			"if your collector is written in XQuery, as the script that is generated via Entity Name and Flow Name assumes that the collector is written in JavaScript.")
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
		.defaultValue(IDENTIFIERS_ATTRIBUTE)
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
		list.add(DHF_VERSION);
		list.add(STEP);
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
		
		DatabaseClient client = getDatabaseClient(context);
	
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			flowFile = session.create();
		}

		EvalResultIterator iterator =  null;
		BufferedReader bufferedReader = null;
		
		final String dhfVersion = context.getProperty(DHF_VERSION).evaluateAttributeExpressions(flowFile).getValue();
				
		final String scriptBody = context.getProperty(SCRIPT_BODY).evaluateAttributeExpressions(flowFile).getValue();

		if (scriptBody != null && scriptBody.trim().length() > 0) {												
			iterator = evaluateScript(context, flowFile, client, scriptBody);					
		}
		else {
			if (dhfVersion.equals("4")) {
					try {    	
				    	bufferedReader = invokeRestEndpoint4(context, flowFile, client);
					}
					catch (Exception ex) {
						transferAndCommit(session, flowFile, FAILURE);
					}			
			}
			
			else if (dhfVersion.equals("5")){
					try {    	
				    	bufferedReader = invokeRestEndpoint5(context, flowFile, client);
					}
					catch (Exception ex) {
						transferAndCommit(session, flowFile, FAILURE);
					}	
			}
			
			else {
				getLogger().error("Provided DHF Version is invalid");
				transferAndCommit(session, flowFile, FAILURE);
			}
			
		}
						
		try {
			createFlowFilesForBatchesOfIdentifiers(context, session, flowFile, iterator,bufferedReader);
			transferAndCommit(session, flowFile, ORIGINAL);
		} 
		catch (Exception ex) {
			transferAndCommit(session, flowFile, FAILURE);
		}
		
		
		finally {
			 if (iterator != null ) { iterator.close(); }
			try {
			  if (bufferedReader != null ) { 
				  bufferedReader.close(); 
			  }
			} catch (IOException e) {			
				transferAndCommit(session, flowFile, FAILURE);
			}
		}	
	} 
			
	/**
	 * @param context
	 * @param flowFile
	 * @param db
	 * @return
	 * @throws IOException
	 */
	protected BufferedReader invokeRestEndpoint5(ProcessContext context, FlowFile flowFile, DatabaseClient db) throws IOException {
		
	    	OkHttpClient client = (OkHttpClient) db.getClientImplementation();
	    	
	    	String protocol = db.getSecurityContext().getSSLContext() != null ? "https" : "http";
	    	String host = db.getHost();
	    	int port = db.getPort();		    	
	    	String endPoint = "/v1/internal/hubcollector5";
	    	
			final String flowName = context.getProperty(FLOW_NAME).evaluateAttributeExpressions(flowFile).getValue();
			final String stepNo = context.getProperty(STEP).evaluateAttributeExpressions(flowFile).getValue();
			String database = db.getDatabase();
	    	
			String uriString = String.format(
	                "%s://%s:%d%s?flow-name=%s&database=%s&step=%s",
	                protocol,
	                host,
	                port,
	                endPoint,
	                flowName,
	                database,
	                stepNo
	            );
			
	    	Request request = new Request.Builder()
	    		      			.url(uriString)
	    		      			.build();
	    		 
	    	Call call = client.newCall(request);
	    	Response response = call.execute();		    	
	    	InputStream is = response.body().byteStream();    	
	    	return new BufferedReader(new InputStreamReader(is));	    
	}

	protected BufferedReader invokeRestEndpoint4(ProcessContext context, FlowFile flowFile, DatabaseClient db) throws IOException {
		
    	OkHttpClient client = (OkHttpClient) db.getClientImplementation();
    	
    	String protocol = db.getSecurityContext().getSSLContext() != null ? "https" : "http";
    	String host = db.getHost();
    	int port = db.getPort();		    	
    	String endPoint = "/v1/internal/hubcollector";
    	    	
        String jobId = UUID.randomUUID().toString();
		final String entityName = context.getProperty(ENTITY_NAME).evaluateAttributeExpressions(flowFile).getValue();
		final String flowName = context.getProperty(FLOW_NAME).evaluateAttributeExpressions(flowFile).getValue();
		String database = db.getDatabase();
		final String options = context.getProperty(OPTIONS).evaluateAttributeExpressions(flowFile).getValue();
    	
        String uriString = String.format(
                "%s://%s:%d%s?job-id=%s&entity-name=%s&flow-name=%s&database=%s&options=%s",
                protocol,
                host,
                port,             
                endPoint,
                jobId,
                entityName,
                flowName,
                database,
                new StringHandle(options).withFormat(Format.JSON)
            );
        
    	Request request = new Request.Builder()
    		      			.url(uriString)
    		      			.build();
    		 
    	Call call = client.newCall(request);
    	Response response = call.execute();		    	
    	InputStream is = response.body().byteStream();    	
    	BufferedReader bufferedReader =  new BufferedReader(new InputStreamReader(is));
    	
    	return bufferedReader;
}
			

	protected EvalResultIterator evaluateScript(ProcessContext context, FlowFile flowFile, DatabaseClient client, String script) {
		final String scriptType = context.getProperty(SCRIPT_TYPE).evaluateAttributeExpressions(flowFile).getValue();
		final String options = context.getProperty(OPTIONS).evaluateAttributeExpressions(flowFile).getValue();

		ServerEvaluationCall call = client.newServerEval();
		call = SCRIPT_TYPE_XQUERY.equals(scriptType) ? call.xquery(script) : call.javascript(script);

		if (options != null && options.trim().length() > 0) {
			getLogger().info("Collector options: " + options);
			call.addVariable("options", new StringHandle(options).withFormat(Format.JSON));
		}

		return call.eval();
	}

	/**
	 * Similar to SplitText, if SET_FRAGMENT_COUNT is set to true, then this needs to generate all of the new FlowFile
	 * objects first, and then set fragment.count on them before transferring any of them.
	 *
	 * @param context
	 * @param session
	 * @param flowFile
	 * @param iterator
	 * @throws IOException 
	 */
	protected void createFlowFilesForBatchesOfIdentifiers(
				ProcessContext context, 
				ProcessSession session, 
				FlowFile flowFile, 
				EvalResultIterator iterator,
				BufferedReader bufferedReader)  
	{
				
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
		
		if (bufferedReader != null) {
		       String line;
		       try {
				while ((line = bufferedReader.readLine()) != null) {
						final String identifier = line;
						getLogger().info(identifier);
						if (delimitedIdentifiers != null) {
							delimitedIdentifiers = delimitedIdentifiers + delimiter + identifier;
						} else {
							delimitedIdentifiers = identifier;
						}
						identifierCount++;
						totalIdentifierCount++;
						// If our count of identifiers equals our batch size, and we don't need to set the fragment count, then
						// we'll transfer a new FlowFile; otherwise we create one and add it to the list to process later
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
				} catch (IOException e) {				
					transferAndCommit(session, flowFile, FAILURE);
				}
			}
			
		else {
			// Iterate over all of the identifiers returned by the collector module
			while (iterator.hasNext()) {
				final String identifier = iterator.next().getString();
				getLogger().info(identifier);
				if (delimitedIdentifiers != null) {
					delimitedIdentifiers = delimitedIdentifiers + delimiter + identifier;
				} else {
					delimitedIdentifiers = identifier;
				}
				identifierCount++;
				totalIdentifierCount++;
				// If our count of identifiers equals our batch size, and we don't need to set the fragment count, then
				// we'll transfer a new FlowFile; otherwise we create one and add it to the list to process later
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
		}
			
		// If we have any identifiers left (because the current batch never equaled batchSize), then process them
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
				attributes.put(IDENTIFIERS_TOTAL_ATTRIBUTE, collectorIdentifiersCount);
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
	 * Construct a new FlowFile with the delimited identifiers stored as an attribute.
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
}
