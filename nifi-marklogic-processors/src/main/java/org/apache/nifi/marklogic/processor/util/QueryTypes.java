package org.apache.nifi.marklogic.processor.util;

import org.apache.nifi.components.AllowableValue;

public class QueryTypes {
    public static final String COLLECTION_STR = "Collection Query";
    public static final AllowableValue COLLECTION = new AllowableValue(COLLECTION_STR, COLLECTION_STR,
        "Comma-separated list of collections to query");
    public static final String COMBINED_JSON_STR = "Combined Query (JSON)";
    public static final AllowableValue COMBINED_JSON = new AllowableValue(COMBINED_JSON_STR, COMBINED_JSON_STR,
        "Combine a string or structured query with dynamic query options; also supports JSON serialized CTS queries");
    public static final String COMBINED_XML_STR = "Combined Query (XML)";
    public static final AllowableValue COMBINED_XML = new AllowableValue(COMBINED_XML_STR, COMBINED_XML_STR,
        "Combine a string or structured query with dynamic query options; also supports XML serialized CTS queries");
    public static final String STRING_STR = "String Query";
    public static final AllowableValue STRING = new AllowableValue(STRING_STR, STRING_STR,
        "A string that utilizes the MarkLogic search grammar for expressing a query");
    public static final String STRUCTURED_JSON_STR = "Structured Query (JSON)";
    public static final AllowableValue STRUCTURED_JSON = new AllowableValue(STRUCTURED_JSON_STR,
        STRUCTURED_JSON_STR,
        "A simple and easy way to construct queries as a JSON structure, allowing you to manipulate complex queries");
    public static final String STRUCTURED_XML_STR = "Structured Query (XML)";
    public static final AllowableValue STRUCTURED_XML = new AllowableValue(STRUCTURED_XML_STR, STRUCTURED_XML_STR,
        "A simple and easy way to construct queries as a XML structure, allowing you to manipulate complex queries");

    public static final AllowableValue[] allValues = new AllowableValue[]{COLLECTION, COMBINED_JSON, COMBINED_XML,
        STRING, STRUCTURED_JSON, STRUCTURED_XML};

}
