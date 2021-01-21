package org.apache.nifi.marklogic.processor.transformers;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

import javax.json.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.InputStream;
import java.io.StringWriter;
import java.text.ParseException;

/**
 * Handles converting Provenance Lineage data (JSON formatted) into RDF/XML formatted triples
 */
public class RdfXmlTransformer extends AbstractTransformer {

	public RdfXmlTransformer(InputStream inputStream, String defNamespace, String defPrefix) {
		super(inputStream, defNamespace, defPrefix);
	}

	/**
	 * Entry point for the conversion process.
	 * @return String representation of the RDF/XML content
	 * @throws ParserConfigurationException
	 * @throws ParseException
	 * @throws TransformerException
	 */
	public String convert() throws ParserConfigurationException, ParseException, TransformerException {
		DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = builderFactory.newDocumentBuilder();

		Document doc = builder.newDocument();

		Element rootElement = doc.createElementNS("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:RDF");
		rootElement.setAttribute("xmlns:owl", "http://www.w3.org/2002/07/owl#");
		rootElement.setAttribute("xmlns:" + DEFAULT_PREFIX, DEFAULT_NAMESPACE);
		rootElement.setAttribute("xmlns:rdfs", "http://www.w3.org/2000/01/rdf-schema#");
		rootElement.setAttribute("xmlns:spin", "http://spinrdf.org/spin#");
		doc.appendChild(rootElement);

		for (JsonValue jsonObject1 : nodesArray) {
			rootElement.appendChild(buildTriple(doc, (JsonObject) jsonObject1, linksArray));
		}

		//Convenience for debugging output to console
		//formatTriples(doc);

		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer = tf.newTransformer();
		StringWriter writer = new StringWriter();
		transformer.transform(new DOMSource(doc), new StreamResult(writer));
		return writer.getBuffer().toString();
	}

	/**
	 * Convenience method for outputting the constructed RDF/XML to the console
	 * @param doc
	 */
	private void formatTriples(Document doc) {
		try {
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			DOMSource source = new DOMSource(doc);
			StreamResult console = new StreamResult(System.out);
			transformer.transform(source, console);
		} catch (TransformerException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Builds the individual triple sections of the document
	 * @param doc A Document used to construct the element
	 * @param provenance The JsonObject containing the parsed Provenance data
	 * @param links An array of Provenance Lineage links representing the flow of provenance events
	 * @return A single XML formatted node representing a triple
	 * @throws ParseException
	 */
	private Node buildTriple(Document doc, JsonObject provenance, JsonArray links) throws ParseException {
		Element baseElement;

		baseElement = doc.createElement(DEFAULT_PREFIX + ":" + getEventType(provenance));
		baseElement.setAttribute("rdf:about", DEFAULT_NAMESPACE + provenance.getString("id"));

		Element mills = buildMills(doc, String.valueOf(provenance.getInt("millis")));
		Element timestamp = buildTimestamp(doc, provenance);
		Element id = buildId(doc, provenance);
		Element ffid = buildFlowFileUuid(doc, provenance);

		baseElement.appendChild(mills);
		baseElement.appendChild(timestamp);
		baseElement.appendChild(id);
		baseElement.appendChild(ffid);

		if (provenance.containsKey("parentUuids") && !provenance.getJsonArray("parentUuids").isEmpty()) {
			JsonArray parentArray = provenance.getJsonArray("parentUuids");
			for (JsonValue val : parentArray) {
				Element el = doc.createElement(DEFAULT_PREFIX + ":parentUuids");
				Text txt = doc.createTextNode(val.toString());
				el.appendChild(txt);
				baseElement.appendChild(el);
			}
		}

		if (provenance.containsKey("childUuids") && !provenance.getJsonArray("childUuids").isEmpty()) {
			JsonArray childArray = provenance.getJsonArray("childUuids");
			for (JsonValue val : childArray) {
				Element el = doc.createElement(DEFAULT_PREFIX + ":childUuids");
				Text txt = doc.createTextNode(val.toString());
				el.appendChild(txt);
				baseElement.appendChild(el);
			}
		}

		String targetEventId = getTargetEventId(links.getValuesAs(JsonObject.class), provenance.getString("id"));

		if (targetEventId != null) {
			Element transTo = buildTransTo(doc, targetEventId);
			baseElement.appendChild(transTo);
		}
		return baseElement;
	}

	/**
	 * Builds the transitionTo element
	 * @param doc A Document used to construct the element
	 * @param targetEventId The event id in the lineage data representing the target to the next triple
	 * @return A transitionTo triple element
	 */
	private Element buildTransTo(Document doc, String targetEventId) {
		Element transTo = doc.createElement(DEFAULT_PREFIX + ":transitionedTo");
		transTo.appendChild(doc.createTextNode(DEFAULT_NAMESPACE + targetEventId));
		return transTo;
	}

	/**
	 * Builds the id element
	 * @param doc A Document used to construct the element
	 * @param provenance The JsonObject containing the parsed Provenance data
	 * @return An id triple element
	 */
	private Element buildId(Document doc, JsonObject provenance) {
		Element id = doc.createElement(DEFAULT_PREFIX + ":id");
		id.appendChild(doc.createTextNode(provenance.getString("id")));
		return id;
	}

	/**
	 * Builds the flowFileUuid element
	 * @param doc A Document used to construct the element
	 * @param provenance The JsonObject containing the parsed Provenance data
	 * @return A flowFileUuid triple element
	 */
	private Element buildFlowFileUuid(Document doc, JsonObject provenance) {
		Element ffid = doc.createElement(DEFAULT_PREFIX + ":flowFileUuid");
		ffid.appendChild(doc.createTextNode(provenance.getString("flowFileUuid")));
		return ffid;
	}

	/**
	 * Builds the timestamp element
	 * @param doc A Document used to construct the element
	 * @param provenance The JsonObject containing the parsed Provenance data
	 * @return A timestamp triple element
	 * @throws ParseException
	 */
	private Element buildTimestamp(Document doc, JsonObject provenance) throws ParseException {
		Element timestamp = doc.createElement(DEFAULT_PREFIX + ":timestamp");
		timestamp.appendChild(doc.createTextNode(formatTimestamp(provenance.getString("timestamp"))));
		timestamp.setAttribute("rdf:datatype", "http://www.w3.org/2001/XMLSchema#dateTime");
		return timestamp;
	}

	/**
	 * Builds the millis element
	 * @param doc
	 * @param mills
	 * @return A millis triples element
	 */
	private Element buildMills(Document doc, String mills) {
		Element millsElement = doc.createElement(DEFAULT_PREFIX + ":millis");
		millsElement.setAttribute("rdf:datatype", "http://www.w3.org/2001/XMLSchema#integer");
		millsElement.appendChild(doc.createTextNode(mills));
		return millsElement;
	}

}
