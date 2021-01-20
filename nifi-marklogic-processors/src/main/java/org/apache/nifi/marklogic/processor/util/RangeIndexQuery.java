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
package org.apache.nifi.marklogic.processor.util;

import java.util.Collection;
import java.util.regex.Matcher;

import javax.xml.namespace.QName;

import org.apache.nifi.marklogic.processor.QueryMarkLogic.IndexTypes;
import org.apache.nifi.util.EscapeUtils;

import com.marklogic.client.io.Format;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryBuilder.Operator;
import com.marklogic.client.query.StructuredQueryBuilder.RangeIndex;

public class RangeIndexQuery {
    private String value;
    private Operator operator;
    private String dataType;
    private String rangeIndexValue;
    private String rangeIndexType;
    private StructuredQueryBuilder queryBuilder;

    public RangeIndexQuery(StructuredQueryBuilder queryBuilder, String rangeIndexType, String rangeIndexValue,
            String dataType, Operator operator, String value) {
        this.rangeIndexType = rangeIndexType;
        this.rangeIndexValue = rangeIndexValue;
        this.queryBuilder = queryBuilder;
        this.setDataType(dataType);
        this.setOperator(operator);
        this.setValue(value);
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String structuredOperatorToCtsOperator(String operator) {
        String ctsOperator;
        switch (operator) {
        case "LT":
            ctsOperator = "<";
            break;
        case "LE":
            ctsOperator = "<=";
            break;
        case "GT":
            ctsOperator = ">";
            break;
        case "GE":
            ctsOperator = ">=";
            break;
        case "EQ":
            ctsOperator = "=";
            break;
        case "NE":
            ctsOperator = "!=";
            break;
        default:
            ctsOperator = operator;
            break;
        }
        return ctsOperator;
    }

    public String toCtsQuery(Format format) {
        StringBuilder strBuilder = new StringBuilder();
        if (format == Format.XML) {
            strBuilder.append("<cts:range-query operator=\"")
                    .append(EscapeUtils.escapeHtml(structuredOperatorToCtsOperator(operator.toString())))
                    .append("\" xmlns:cts=\"http://marklogic.com/cts\">\n");
            switch (rangeIndexType) {
            case IndexTypes.ELEMENT_STR:
                boolean hasNamespace = rangeIndexValue.contains(":");
                String[] parts = rangeIndexValue.split(":", 2);
                String name = (hasNamespace) ? parts[1] : rangeIndexValue;
                String ns = (hasNamespace) ? queryBuilder.getNamespaces().getNamespaceURI(parts[0]) : "";
                strBuilder.append("  <cts:element-reference><cts:localname>").append(name)
                        .append("</cts:localname><cts:scalar-type>")
                        .append(dataType.replace(Matcher.quoteReplacement("xs:"), ""))
                        .append("</cts:scalar-type><cts:namespace-uri>").append(ns)
                        .append("</cts:namespace-uri></cts:element-reference>");
                break;
            case IndexTypes.JSON_PROPERTY_STR:
                strBuilder.append("  <cts:json-property-reference><cts:property>").append(rangeIndexValue)
                        .append("</cts:property><cts:scalar-type>")
                        .append(dataType.replace(Matcher.quoteReplacement("xs:"), "")).append("</cts:scalar-type>")
                        .append("</cts:json-property-reference>");
                break;
            case IndexTypes.PATH_STR:
                strBuilder.append("<cts:path-reference ");
                Collection<String> prefixes = queryBuilder.getNamespaces().getAllPrefixes();
                for (String prefix : prefixes) {
                    strBuilder.append(" xmlns:").append(prefix).append("=\"")
                            .append(queryBuilder.getNamespaces().getNamespaceURI(prefix)).append("\"");
                }
                strBuilder.append("><cts:path-expression>").append(rangeIndexValue)
                        .append("</cts:path-expression><cts:scalar-type>")
                        .append(dataType.replace(Matcher.quoteReplacement("xs:"), "")).append("</cts:scalar-type>")
                        .append("</cts:path-reference>");
                break;
            default:
                break;
            }
            strBuilder.append("<cts:value xsi:type=\"").append(dataType).append(
                    "\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">")
                    .append(value).append("</cts:value></cts:range-query>");
        } else {
            strBuilder.append("{");
            switch (rangeIndexType) {
            case IndexTypes.ELEMENT_STR:
                boolean hasNamespace = rangeIndexValue.contains(":");
                String[] parts = rangeIndexValue.split(":", 2);
                String name = (hasNamespace) ? parts[1] : rangeIndexValue;
                String ns = (hasNamespace) ? queryBuilder.getNamespaces().getNamespaceURI(parts[0]) : "";
                strBuilder.append("\"elementRangeQuery\":{\"element\":[\"{").append(ns).append("}").append(name)
                        .append("\"],");
                break;
            case IndexTypes.JSON_PROPERTY_STR:
                strBuilder.append("\"jsonPropertyRangeQuery\":{\"property\":[\"").append(rangeIndexValue)
                        .append("\"],");
                break;
            case IndexTypes.PATH_STR:
                strBuilder.append("\"pathRangeQuery\":{\"pathExpression\":[\"").append(rangeIndexValue).append("\"],");
                break;
            default:
                break;
            }
            strBuilder.append("\"operator\":\"").append(structuredOperatorToCtsOperator(operator.toString()))
                    .append("\", \"value\":[{\"type\":\"").append(dataType.replace(Matcher.quoteReplacement("xs:"), ""))
                    .append("\", \"val\":\"").append(value).append("\"}]}}");
        }
        return strBuilder.toString();
    }

    public String toStructuredQuery(Format format) {
        if (format == Format.XML) {
            RangeIndex rangeIndex = null;
            switch (rangeIndexType) {
            case IndexTypes.ELEMENT_STR:
                boolean hasNamespace = rangeIndexValue.contains(":");
                String[] parts = rangeIndexValue.split(":", 2);
                String name = (hasNamespace) ? parts[1] : rangeIndexValue;
                String ns = (hasNamespace) ? queryBuilder.getNamespaces().getNamespaceURI(parts[0]) : "";
                rangeIndex = queryBuilder.element(new QName(ns, name));
                break;
            case IndexTypes.JSON_PROPERTY_STR:
                rangeIndex = queryBuilder.jsonProperty(rangeIndexValue);
                break;
            case IndexTypes.PATH_STR:
                rangeIndex = queryBuilder.pathIndex(rangeIndexValue);
                break;
            default:
                break;
            }
            return queryBuilder.range(rangeIndex, this.getDataType(), this.getOperator(), this.getValue()).serialize();
        } else {
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append("{ \"range-query\": { \"type\": \"").append(dataType).append("\",");
            switch (rangeIndexType) {
            case IndexTypes.ELEMENT_STR:
                boolean hasNamespace = rangeIndexValue.contains(":");
                String[] parts = rangeIndexValue.split(":", 2);
                String name = (hasNamespace) ? parts[1] : rangeIndexValue;
                String ns = (hasNamespace) ? queryBuilder.getNamespaces().getNamespaceURI(parts[0]) : "";
                strBuilder.append("\"element\": { \"name\": \"").append(name).append("\", \"ns\": \"").append(ns)
                        .append("\"}");
                break;
            case IndexTypes.JSON_PROPERTY_STR:
                strBuilder.append("\"json-property\": \"").append(rangeIndexValue).append("\"");
                break;
            case IndexTypes.PATH_STR:
                strBuilder.append("\"path-index\": { \"text\": \"").append(rangeIndexValue).append("\",")
                        .append("\"namespaces\": {");
                Collection<String> prefixes = queryBuilder.getNamespaces().getAllPrefixes();
                int prefixIndex = 0;
                int prefixCount = prefixes.size();
                for (String prefix : prefixes) {
                    prefixIndex++;
                    strBuilder.append("\"").append(prefix).append("\": \"")
                            .append(queryBuilder.getNamespaces().getNamespaceURI(prefix)).append("\"");
                    if (prefixIndex != prefixCount) {
                        strBuilder.append(",");
                    }
                }
                strBuilder.append("}}");
                break;
            default:
                break;
            }
            strBuilder.append(", \"value\": \"").append(value).append("\", \"range-operator\": \"").append(operator)
                    .append("\" }}");
            return strBuilder.toString();
        }
    }
}
