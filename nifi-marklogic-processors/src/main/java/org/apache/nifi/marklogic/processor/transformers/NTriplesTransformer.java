package org.apache.nifi.marklogic.processor.transformers;


import javax.json.*;
import java.io.InputStream;
import java.text.ParseException;

/**
 * Class responsible for handling the conversion of Provenance Lineage (JSON formatted) data into N-Triples format
 */
public class NTriplesTransformer extends AbstractTransformer {

	public NTriplesTransformer(InputStream inputStream, String defNamespace, String defPrefix){
		super(inputStream, defNamespace, defPrefix);
	}

	/**
	 * Entry point for the conversion process.
	 * @return String representation of the N-Triples content
	 * @throws ParseException
	 */
	public String convert() throws ParseException {
		StringBuffer builder = new StringBuffer();

		for (JsonValue jsonObject1 : nodesArray) {
			buildTriple(builder, (JsonObject) jsonObject1, linksArray);
		}

		return builder.toString();
	}

	/**
	 * Builds the individual triple sections of the document
	 * @param builder A StringBuffer object to build upon
	 * @param provenance The JsonObject containing the parsed Provenance data
	 * @param links An array of Provenance Lineage links representing the flow of provenance events
	 * @throws ParseException
	 */
	private void buildTriple(StringBuffer builder, JsonObject provenance, JsonArray links) throws ParseException {
		String targetEventId = getTargetEventId(links.getValuesAs(JsonObject.class), provenance.getString("id"));
		String provEventString = "<"+DEFAULT_NAMESPACE + provenance.getString("id") + ">";
		builder
				//eventType element
				.append(provEventString)
				.append(" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ")
				.append(" <")
				.append(DEFAULT_NAMESPACE)
				.append(getEventType(provenance))
				.append("> .")
				.append(separator)

				//ID element
				.append(provEventString).append(" <").append(DEFAULT_NAMESPACE).append("id> ").append("\"")
				.append(provenance.getString("id")).append("\" .").append(separator)

				//flowFileUuid element
				.append(provEventString).append(" <").append(DEFAULT_NAMESPACE).append("flowFileUuid> ").append("\"")
				.append(provenance.getString("flowFileUuid")).append("\" .").append(separator)

				// millis element
				.append(provEventString).append(" <").append(DEFAULT_NAMESPACE).append("millis> ").append("\"")
				.append(provenance.getInt("millis")).append("\"")
				.append("^^<http://www.w3.org/2001/XMLSchema#integer> .").append(separator)

				//timestamp element
				.append(provEventString).append(" <").append(DEFAULT_NAMESPACE).append("timestamp> ").append("\"")
				.append(formatTimestamp(provenance.getString("timestamp"))).append("\"")
				.append("^^<http://www.w3.org/2001/XMLSchema#dateTime> .").append(separator);

		//childUuids elements (if exists and is not empty)
		if (provenance.containsKey("childUuids") && !provenance.getJsonArray("childUuids").isEmpty()) {
			JsonArray entryArray = provenance.getJsonArray("childUuids");
			for (JsonValue childUuid : entryArray) {
				builder.append(provEventString).append(" <").append(DEFAULT_NAMESPACE).append("childUuids> ")
						.append(childUuid).append(" .").append(separator);
			}
		}

		//parentUuids element (if exists and is not empty)
		if (provenance.containsKey("parentUuids") && !provenance.getJsonArray("parentUuids").isEmpty()) {
			JsonArray entryArray = provenance.getJsonArray("parentUuids");
			for (JsonValue parentUuid : entryArray) {
				builder.append(provEventString).append(" <").append(DEFAULT_NAMESPACE).append("parentUuids> ")
						 .append(parentUuid).append(" .").append(separator);
			}
		}

		//transferredTo element (if one exists)
		if (targetEventId != null)
			builder
					.append(provEventString)
					.append(" <")
					.append(DEFAULT_NAMESPACE)
					.append("transitionedTo> ")
					.append(" <")
					.append(DEFAULT_NAMESPACE)
					.append(targetEventId)
					.append("> .")
					.append(separator);
	}
}
