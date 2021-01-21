package org.apache.nifi.marklogic.processor.transformers;

import org.apache.commons.text.CaseUtils;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * This is the abstract Transform class that all other RDF Transforms extend and provides common functionality.
 */
public abstract class AbstractTransformer {
	final String DEFAULT_NAMESPACE;
	final String DEFAULT_PREFIX;
	final JsonArray nodesArray;
	final JsonArray linksArray;
	final JsonObject jsonObject;
	final String separator = System.getProperty("line.separator");

	public AbstractTransformer(InputStream inputStream, String defNamespace, String defPrefix) {
		this.DEFAULT_NAMESPACE = (defNamespace.endsWith("/") || defNamespace.endsWith("#")) ? defNamespace : defNamespace + "/";
		this.DEFAULT_PREFIX = defPrefix;
		jsonObject = readJsonFromStream(inputStream);
		nodesArray = jsonObject.getJsonObject("lineage").getJsonObject("results").getJsonArray("nodes");
		linksArray = jsonObject.getJsonObject("lineage").getJsonObject("results").getJsonArray("links");
	}

	String convert() throws ParserConfigurationException, ParseException, TransformerException {
		return null;
	}

	/**
	 * Utility method to properly format the timestamp field
	 * @param dateToFormat The string date to format
	 * @return A correctly formatted timestamp yyyy-MM-dd'T'HH:mm:ss.SSS ZZZ
	 * @throws ParseException
	 */
	String formatTimestamp(String dateToFormat) throws ParseException {
		SimpleDateFormat fromFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS ");
		SimpleDateFormat toFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS ZZZ");
		return toFormat.format(fromFormat.parse(dateToFormat));
	}

	/**
	 * Utility method for finding the event that a previous event links to
	 * @param links A List of links (source & target) identifying which events are linked
	 * @param eventId The source ID of an event
	 * @return The Target ID of an event's Source ID
	 */
	String getTargetEventId(final List<JsonObject> links, final String eventId) {
		String targetEventId = null;

		boolean containsEventKey =
				links.stream().anyMatch(e -> e.containsKey("sourceId") && e.getString("sourceId").equals(eventId));

		if (containsEventKey)
			targetEventId =
					links
							.stream()
							.filter(e -> e.getString("sourceId").equals(eventId))
							.findFirst()
							.get()
							.getString("targetId");

		return targetEventId;
	}

	/**
	 * Reads the incoming Json input from the flow file and creates a JsonObject
	 * @param inputStream The input flow file as an input stream
	 * @return A JsonObject of the input
	 */
	private JsonObject readJsonFromStream(InputStream inputStream) {
		JsonReader reader = Json.createReader(inputStream);
		JsonObject jsonObject = reader.readObject();
		reader.close();
		return jsonObject;
	}

	/**
	 * Common method used to correctly determin and format the Event Type string
	 * @param provenance A JsonObject representing a single Provenance event
	 * @return Correctly Formatted Event Type (AttributeUpdated, Flowfile, Fork, etc.)
	 */
	String getEventType(JsonObject provenance){
		String evtType = provenance.containsKey("eventType") ? provenance.getString("eventType") : provenance.getString("type");
		return CaseUtils.toCamelCase(evtType.replace("_", " "), true).replace(" ", "");
	}
}
