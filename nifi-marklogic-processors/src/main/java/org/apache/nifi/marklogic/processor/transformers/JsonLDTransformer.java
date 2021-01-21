package org.apache.nifi.marklogic.processor.transformers;


import javax.json.*;
import java.io.InputStream;
import java.text.ParseException;

/**
 * Class handles transforming Provenance Lineage (Json) data into JSON-LD Triples format
 */
public class JsonLDTransformer extends AbstractTransformer {
	private String namespaceOverride;

	public JsonLDTransformer(InputStream inputStream, String defNamespace, String defPrefix){
		super(inputStream, defNamespace, defPrefix);
		namespaceOverride = super.DEFAULT_NAMESPACE.endsWith("/") ? super.DEFAULT_NAMESPACE.substring(0, super.DEFAULT_NAMESPACE.length()-1) : super.DEFAULT_NAMESPACE;
	}

	/**
	 * Main entry point for transformation process
	 * @return String representation of the JSON-LD formatted content
	 * @throws ParseException Thrown if exception occurs during Json parsing of source content
	 */
	public String convert() throws ParseException {
		JsonArrayBuilder lineageArray = Json.createArrayBuilder();

		for (JsonValue jsonObject1 : nodesArray) {
			lineageArray.add(buildTriple((JsonObject) jsonObject1, linksArray));
		}

		lineageArray.add(buildOntologyBlock(jsonObject.getJsonObject("lineage").getString("id")));
		//JsonArray l = lineageArray.build();
		JsonObject lineageObject =
				Json.createObjectBuilder().add("@graph", lineageArray)
						.add("@context", buildContextBlock())
						.build();

		return lineageObject.toString();
	}

	/**
	 * Builds individual triples for each provenance event node in the source document
	 * @param provenance Single JsonObject of Provenance Lineage data event
	 * @param links Source Json Provenance Lineage data (links)
	 * @return JsonObject representing the transformed triple into JSON-LD format
	 * @throws ParseException Thrown if exception occurs during Json parsing of source content
	 */
	private JsonObject buildTriple(JsonObject provenance, JsonArray links) throws ParseException {
		JsonObjectBuilder prov = Json.createObjectBuilder();

		prov.add("@id", DEFAULT_PREFIX + ":" + provenance.getString("id"));
		prov.add("@type", DEFAULT_PREFIX + ":" + getEventType(provenance));
		prov.add(DEFAULT_PREFIX + ":flowFileUuid", provenance.getString("flowFileUuid"));

		if (provenance.containsKey("childUuids") && !provenance.getJsonArray("childUuids").isEmpty())
			prov.add(DEFAULT_PREFIX + ":childUuids", Json.createObjectBuilder().add("@set", provenance.getJsonArray("childUuids")));


		prov.add(DEFAULT_PREFIX + ":id", provenance.getString("id"));
		prov.add(DEFAULT_PREFIX + ":millis", provenance.getInt("millis"));
		if (provenance.containsKey("parentUuids") && !provenance.getJsonArray("parentUuids").isEmpty())
			prov.add(DEFAULT_PREFIX +":parentUuids", Json.createObjectBuilder().add("@set", provenance.getJsonArray("parentUuids")));

		prov.add(DEFAULT_PREFIX +":timestamp", formatTimestamp(provenance.getString("timestamp")));

		String targetEventId = getTargetEventId(links.getValuesAs(JsonObject.class), provenance.getString("id"));

		if (targetEventId != null)
			prov.add(DEFAULT_PREFIX +":transitionedTo", DEFAULT_PREFIX + ":" + targetEventId);
		return prov.build();
	}

	/**
	 * Builds out the initial Ontololgy block for the document
	 * @param lineageId Id for the Lineage query performed
	 * @return JsonObjectBuilder object to be added to transformed document
	 */
	private JsonObjectBuilder buildOntologyBlock(String lineageId) {
		return
				Json.createObjectBuilder()
						.add("@id", namespaceOverride + "/" + DEFAULT_PREFIX + "#" + lineageId)
						.add("@type", "owl:Ontology")
						.add("imports", DEFAULT_NAMESPACE);
	}

	/**
	 * Constructs the context block section for the document
	 * @return JsonObjectBuilder object to be added to triples document
	 */
	private JsonObjectBuilder buildContextBlock() {
		return Json.createObjectBuilder()
				.add("imports",
						Json.createObjectBuilder()
								.add("@id", "http://www.w3.org/2002/07/owl#imports")
								.add("@type", "@id"))
				.add("flowFileUuid",
						Json.createObjectBuilder()
								.add("@id", namespaceOverride + "#flowFileUuid"))
				.add("id",
						Json.createObjectBuilder()
								.add("@id", namespaceOverride + "#id"))
				.add("millis",
						Json.createObjectBuilder()
								.add("@id", namespaceOverride + "#millis"))
				.add("timestamp",
						Json.createObjectBuilder()
								.add("@id", namespaceOverride + "#timestamp")
								.add("@type", "http://www.w3.org/2001/XMLSchema#dateTime"))
				.add("transitionedTo",
						Json.createObjectBuilder()
								.add("@id", namespaceOverride + "#transitionedTo")
								.add("@type", "@id"))
				.add("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
				.add("owl", "http://www.w3.org/2002/07/owl#")
				.add("xsd", "http://www.w3.org/2001/XMLSchema#")
				.add("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
				.add(DEFAULT_PREFIX, namespaceOverride + "#");
	}
}
