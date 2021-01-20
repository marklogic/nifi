package org.apache.nifi.marklogic.processor.transformers;


import javax.json.*;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Map;

/**
 * Class handles transforming Provenance Lineage (Json) data into RDF+JSON Triples format
 */
public class RdfJsonTransformer extends AbstractTransformer {

	public RdfJsonTransformer(InputStream inputStream, String defNamespace, String defPrefix){
		super(inputStream, defNamespace, defPrefix);
	}

	/**
	 * Main entry point for transformation process
	 * @return String representation of the RDF+JSON formatted content
	 * @throws ParseException Thrown if exception occurs during Json parsing of source content
	 */
	public String convert() throws ParseException {
		JsonObjectBuilder provBuilder = Json.createObjectBuilder();
		for (JsonValue jsonObject1 : nodesArray) {
			JsonObject parentObj = (JsonObject) jsonObject1;
			JsonObject jObj = buildTriple((JsonObject) jsonObject1, linksArray);
			provBuilder.add(DEFAULT_NAMESPACE  + parentObj.getString("id"), jObj);
		}
		return provBuilder.build().toString();
	}

	/**
	 * Builds individual triples for each provenance event node in the source document
	 * @param provenance Single JsonObject of Provenance Lineage data event
	 * @param links Source Json Provenance Lineage data (links)
	 * @return JsonObject representing the transformed triple into JSON-LD format
	 * @throws ParseException Thrown if exception occurs during Json parsing of source content
	 */
	private JsonObject buildTriple(JsonObject provenance, JsonArray links) throws ParseException {
		String targetEventId = getTargetEventId(links.getValuesAs(JsonObject.class), provenance.getString("id"));

		JsonObjectBuilder builder = Json.createObjectBuilder();
		for (Map.Entry<String, JsonValue> entry : provenance.entrySet()) {
			switch (entry.getKey()) {
				case "id":
					builder.add(DEFAULT_NAMESPACE + entry.getKey(),
							Json.createArrayBuilder()
									.add(
											Json.createObjectBuilder()
													.add("type", "literal")
													.add("value", entry.getValue())
									));
					break;
				case "flowFileUuid":
					builder.add(DEFAULT_NAMESPACE + entry.getKey(),
							Json.createArrayBuilder()
									.add(
											Json.createObjectBuilder()
													.add("type", "literal")
													.add("value", entry.getValue())
									));
					break;
				case "parentUuids":
					if (!provenance.getJsonArray("parentUuids").isEmpty()) {
						JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
						JsonArray entryArray = provenance.getJsonArray("parentUuids");

						for (JsonValue childUuid : entryArray) {
							jsonArrayBuilder.add(
									Json.createObjectBuilder()
											.add("type", "literal")
											.add("value", childUuid)
							);
						}
						builder.add(DEFAULT_NAMESPACE + entry.getKey(), jsonArrayBuilder);
					}
					break;
				case "childUuids":
					if (provenance.containsKey("childUuids") && !provenance.getJsonArray("childUuids").isEmpty()) {
						JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
						JsonArray entryArray = provenance.getJsonArray("childUuids");

						for(JsonValue childUuid : entryArray){
							jsonArrayBuilder.add(
									Json.createObjectBuilder()
											.add("type", "literal")
											.add("value", childUuid)
							);
						}
						builder.add(DEFAULT_NAMESPACE + entry.getKey(), jsonArrayBuilder);
					}
					break;
				case "type":
				case "eventType":
					builder.add("http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
							Json.createArrayBuilder()
									.add(
											Json.createObjectBuilder()
													.add("type", "uri")
													.add("value", DEFAULT_NAMESPACE + getEventType(provenance) + "")
									));
					break;

				case "millis":
					builder.add(DEFAULT_NAMESPACE + entry.getKey(),
							Json.createArrayBuilder()
									.add(
											Json.createObjectBuilder()
													.add("type", "literal")
													.add("value", entry.getValue())
													.add("datatype", "http://www.w3.org/2001/XMLSchema#integer")
									));
					break;
				case "timestamp":
					builder.add(DEFAULT_NAMESPACE + entry.getKey(),
							Json.createArrayBuilder()
									.add(
											Json.createObjectBuilder()
													.add("type", "literal")
													.add("value", formatTimestamp(provenance.getString("timestamp")))//Can't use entry.getValue() because it's double-quoting the string value
													.add("datatype", "http://www.w3.org/2001/XMLSchema#dateTime")
									));
					break;
			}
		}

		if (targetEventId != null)
			builder.add(DEFAULT_NAMESPACE + "transitionedTo",
					Json.createArrayBuilder()
							.add(
									Json.createObjectBuilder()
											.add("type", "uri")
											.add("value", DEFAULT_NAMESPACE + targetEventId)
							));

		return builder.build();
	}

}
