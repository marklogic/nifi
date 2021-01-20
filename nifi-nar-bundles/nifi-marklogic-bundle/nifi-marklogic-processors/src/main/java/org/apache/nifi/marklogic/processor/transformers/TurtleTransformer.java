package org.apache.nifi.marklogic.processor.transformers;


import javax.json.*;
import java.io.InputStream;
import java.text.ParseException;

/**
 * Class handles transforming Provenance Lineage (Json) data into Turtle Triples format
 */
public class TurtleTransformer extends AbstractTransformer {

	public TurtleTransformer(InputStream inputStream, String defNamespace, String defPrefix) {
		super(inputStream, defNamespace, defPrefix);
	}

	/**
	 * Main entry point for transformation process
	 * @return String representation of the Turtle formatted content
	 * @throws ParseException Thrown if exception occurs during Json parsing of source content
	 */
	public String convert() throws ParseException {
		StringBuffer builder = new StringBuffer();

		buildHeader(builder);
		for (JsonValue jsonObject1 : nodesArray) {
			buildTriple(builder, (JsonObject) jsonObject1, linksArray);
		}

		return builder.toString();
	}

	/**
	 * Builds out header section for transformed content
	 * @param builder A StringBuffer object that content is added to
	 */
	private void buildHeader(StringBuffer builder) {
		builder.append("@prefix " + DEFAULT_PREFIX + ": <" + DEFAULT_NAMESPACE + "> .").append(separator)
				.append("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .").append(separator)
				.append("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .").append(separator)
				.append("@prefix owl: <http://www.w3.org/2002/07/owl#> .").append(separator)
				.append("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .").append(separator).append(separator);
	}

	/**
	 * Builds individual triples for each provenance event node in the source document
	 * @param builder A StringBuffer object that content is added to
	 * @param provenance Single JsonObject of Provenance Lineage data event
	 * @param links Source Json Provenance Lineage data (links)
	 * @throws ParseException Thrown if exception occurs during Json parsing of source content
	 */
	private void buildTriple(StringBuffer builder, JsonObject provenance, JsonArray links) throws ParseException {
		builder
				.append("<")
				.append(DEFAULT_NAMESPACE)
				.append(provenance.getString("id"))
				.append(">")
				.append(separator)
				.append("\t")
				.append("a prov:")
				.append(getEventType(provenance))
				.append(" ;")
				.append(separator)
				.append("\t")
				.append(DEFAULT_PREFIX)
				.append(":flowFileUuid \"")
				.append(provenance.getString("flowFileUuid"))
				.append("\" ;")
				.append(separator)
				.append("\t");

		if (provenance.containsKey("childUuids") && !provenance.getJsonArray("childUuids").isEmpty()) {
			JsonArray childArray = provenance.getJsonArray("childUuids");
			childArray.forEach(val -> builder
																		.append(DEFAULT_PREFIX)
																		.append(":childUuids ")
																		.append(val)
																		.append(" ;")
																		.append(separator)
																		.append("\t"));
		}

		builder
				.append("prov:id \"")
				.append(provenance.getString("id"))
				.append("\" ;")
				.append(separator)
				.append("\t")
				.append(DEFAULT_PREFIX)
				.append(":millis ")
				.append(provenance.getInt("millis"))
				.append(" ;")
				.append(separator)
				.append("\t");

		if (provenance.containsKey("parentUuids") && !provenance.getJsonArray("parentUuids").isEmpty()) {
			JsonArray parentArray = provenance.getJsonArray("parentUuids");
			parentArray.forEach(val ->
															builder.append(DEFAULT_PREFIX).append(":parentUuids ").append(val).append(" ;").append(separator).append("\t"));
		}

		builder
				.append(DEFAULT_PREFIX)
				.append(":timestamp \"")
				.append(formatTimestamp(provenance.getString("timestamp")))
				.append("\"^^xsd:dateTime")
				.append(" ;")
				.append(separator);

		String targetEventId = getTargetEventId(links.getValuesAs(JsonObject.class), provenance.getString("id"));

		if (targetEventId != null)
			builder.append("\t").append(DEFAULT_PREFIX).append(":transitionedTo <").append(DEFAULT_NAMESPACE).append(targetEventId).append("> ;").append(separator);

		builder.append(".").append(separator);

	}

}
