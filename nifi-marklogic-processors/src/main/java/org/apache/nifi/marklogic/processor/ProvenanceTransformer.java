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

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.marklogic.processor.transformers.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

@EventDriven
@SideEffectFree
@Tags({"marklogic", "semantics", "provenance", "lineage", "triples", "transform"})
@CapabilityDescription("This processor is designed specifically for transforming Provenance Lineage results (from JSON format) into pre-defined Semantics formats for generating triples")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
		@WritesAttribute(attribute = "mime.type", description = "Updates the mime.type attribute with the correct format for the selected output"),
		@WritesAttribute(attribute = "mime.extension", description = "Updates the mime.extension attribute with the correct value for the selected output"),
		@WritesAttribute(attribute = "Content-Type", description = "Updates/Adds the Content-Type attribute with the correct value for the selected output")
})
public class ProvenanceTransformer extends AbstractProcessor {
	private final static String DEFAULT_CHARSET = "UTF-8";

	public static final PropertyDescriptor OUTPUT_FORMAT =
			new PropertyDescriptor
							.Builder().name("OUTPUT_FORMAT")
					.displayName("Output Format")
					.description("Select the output format for the transform. **NOTE: JSON-LD is NOT a compatible triples format with MarkLogic")
					.allowableValues("JSON-LD", "RDF/JSON", "RDF/XML", "TURTLE", "N-TRIPLES")
					.defaultValue("RDF/XML")
					.required(true)
					.addValidator(Validator.VALID)
					.build();
	public static final PropertyDescriptor DEFAULT_TRIPLE_NAMESPACE =
			new PropertyDescriptor
							.Builder().name("DEFAULT_TRIPLE_NAMESPACE")
					.displayName("Triples Namespace")
					.description("The default namespace to be used for generated triples.")
					.defaultValue("http://marklogic.com/nifi/Provenance")
					.required(false)
					.addValidator(Validator.VALID)
					.build();

	public static final PropertyDescriptor DEFAULT_TRIPLE_NAMESPACE_PREFIX =
			new PropertyDescriptor
							.Builder().name("DEFAULT_TRIPLE_NAMESPACE_PREFIX")
					.displayName("Triples Prefix")
					.description("The default namespace prefix to be used for generated triples.")
					.defaultValue("prov")
					.required(false)
					.addValidator(Validator.VALID)
					.build();


	public static final Relationship SUCCESS = new Relationship.Builder()
																								 .name("Success")
																								 .description("Routes to Success")
																								 .build();

	public static final Relationship FAILURE = new Relationship.Builder()
																								 .name("Failure")
																								 .description("Routes to Failed")
																								 .build();
	public static final Relationship ORIGINAL = new Relationship.Builder()
																									.name("Original")
																									.description("Routes Original")
																									.build();
	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(OUTPUT_FORMAT);
		descriptors.add(DEFAULT_TRIPLE_NAMESPACE);
		descriptors.add(DEFAULT_TRIPLE_NAMESPACE_PREFIX);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(SUCCESS);
		relationships.add(FAILURE);
		relationships.add(ORIGINAL);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			context.yield();
			return;
		}

		final ComponentLog logger = getLogger();
		final String orgFileName = flowFile.getAttribute("filename");
		final String defaultNamespace = context.getProperty("DEFAULT_TRIPLE_NAMESPACE").getValue();
		final String defaultPrefix = context.getProperty("DEFAULT_TRIPLE_NAMESPACE_PREFIX").getValue();

		final StopWatch stopWatch = new StopWatch(true);

		final byte[] content = new byte[(int) flowFile.getSize()];
		session.read(flowFile, inputStream -> StreamUtils.fillBuffer(inputStream, content));

		InputStream is = new ByteArrayInputStream(content);

		FlowFile transformed = session.create(flowFile);
		AbstractTransformer transformer;
		final String result;
		String contentType = "";
		String extension = "";

		try {
			switch (context.getProperty(OUTPUT_FORMAT).getValue()) {
				case "JSON-LD":
					transformer = new JsonLDTransformer(is, defaultNamespace, defaultPrefix);
					result = ((JsonLDTransformer) transformer).convert();
					transformed = session.write(transformed, out -> out.write(result.getBytes(DEFAULT_CHARSET)));
					contentType = "application/ld+json";
					extension = ".jsonld";
					break;
				case "RDF/XML":
					transformer = new RdfXmlTransformer(is, defaultNamespace, defaultPrefix);
					result = ((RdfXmlTransformer) transformer).convert();
					transformed = session.write(transformed, out -> out.write(result.getBytes(DEFAULT_CHARSET)));
					contentType = "application/rdf+xml";
					extension = ".rdf";
					break;
				case "TURTLE":
					transformer = new TurtleTransformer(is, defaultNamespace, defaultPrefix);
					result = ((TurtleTransformer) transformer).convert();
					transformed = session.write(transformed, out -> out.write(result.getBytes(DEFAULT_CHARSET)));
					contentType = "application/x-turtle";
					extension = ".ttl";
					break;
				case "RDF/JSON":
					transformer = new RdfJsonTransformer(is, defaultNamespace, defaultPrefix);
					result = ((RdfJsonTransformer) transformer).convert();
					transformed = session.write(transformed, out -> out.write(result.getBytes(DEFAULT_CHARSET)));
					contentType = "application/rdf+json";
					extension = ".rj";
					break;
				case "N-TRIPLES":
					transformer = new NTriplesTransformer(is, defaultNamespace, defaultPrefix);
					result = ((NTriplesTransformer) transformer).convert();
					transformed = session.write(transformed, out -> out.write(result.getBytes(DEFAULT_CHARSET)));
					contentType = "application/n-triples";
					extension = ".nt";
					break;
			}

			transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), contentType);
			transformed = session.putAttribute(transformed, "Content-Type", contentType);
			transformed = session.putAttribute(transformed, "mime.extension", extension);
			if(orgFileName != null)
				transformed = session.putAttribute(transformed, "filename", orgFileName.substring(0, orgFileName.lastIndexOf('.')) + extension);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Routing to {} due to exception: {}", new Object[]{FAILURE.getName(), e}, e);
			session.transfer(flowFile, FAILURE);
		}

		session.transfer(transformed, SUCCESS);
		session.transfer(flowFile, ORIGINAL);
		session.getProvenanceReporter().modifyContent(transformed, "Content transformed to " + context.getProperty(OUTPUT_FORMAT).getValue(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
		logger.debug("Transformed {}", new Object[]{flowFile});

	}
}
