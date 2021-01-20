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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


import java.io.*;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class ProvenanceTransformerTest extends AbstractProvenanceTest {

    private TestRunner testRunner;
    private InputStream content;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ProvenanceTransformer.class);
        classLoader = getClass().getClassLoader();
        content = classLoader.getResourceAsStream("./inputFiles/sample-lineage.json");

    }

    @Test
    public void testRdfXmlTransformer() {
        testRunner = TestRunners.newTestRunner(new ProvenanceTransformer());
        testRunner.setProperty(ProvenanceTransformer.OUTPUT_FORMAT, "RDF/XML");
        //Validate Namespace and prefix changes are applied
        testRunner.setProperty(ProvenanceTransformer.DEFAULT_TRIPLE_NAMESPACE, "http://example.com/nifi/Boo");
        testRunner.setProperty(ProvenanceTransformer.DEFAULT_TRIPLE_NAMESPACE_PREFIX, "boo");
        testRunner.setVariable("filename", "2017051615_3a4602c201d2ce5d00001750.txt");

        testRunner.enqueue(content);
        testRunner.run(1);
        testRunner.assertQueueEmpty();

        List<MockFlowFile> result = testRunner.getFlowFilesForRelationship(ProvenanceTransformer.SUCCESS);
        assertEquals("1 Transformed to RDF/XML", 1, result.size());
        result.get(0).assertAttributeEquals("mime.extension", ".rdf");
        result.get(0).assertAttributeEquals("mime.type", "application/rdf+xml");
        result.get(0).assertContentEquals(readFileAsString("expected-output/expected-output.rdf"));
    }

    @Test
    public void testJsonLDTransformer() {
        testRunner = TestRunners.newTestRunner(new ProvenanceTransformer());
        testRunner.setProperty(ProvenanceTransformer.OUTPUT_FORMAT, "JSON-LD");
        testRunner.enqueue(content);
        testRunner.run(1);
        testRunner.assertQueueEmpty();

        List<MockFlowFile> result = testRunner.getFlowFilesForRelationship(ProvenanceTransformer.SUCCESS);
        assertEquals("1 Transformed to JSON-LD", 1, result.size());
        result.get(0).assertAttributeEquals("mime.extension", ".jsonld");
        result.get(0).assertAttributeEquals("mime.type", "application/ld+json");
        result.get(0).assertContentEquals(readFileAsString("expected-output/expected-output.jsonld"));
    }

    @Test
    public void testTurtleTransformer() throws IOException {
        testRunner = TestRunners.newTestRunner(new ProvenanceTransformer());
        testRunner.setProperty(ProvenanceTransformer.OUTPUT_FORMAT, "TURTLE");
        testRunner.enqueue(content);
        testRunner.run(1);
        testRunner.assertQueueEmpty();

        List<MockFlowFile> result = testRunner.getFlowFilesForRelationship(ProvenanceTransformer.SUCCESS);
        assertEquals("1 Transformed to Turtle format", 1, result.size());
        result.get(0).assertAttributeEquals("mime.extension", ".ttl");
        result.get(0).assertAttributeEquals("mime.type", "application/x-turtle");
        String text = readFileAsString("expected-output/expected-output.ttl");
        text = text.replace("\n", System.getProperty("line.separator"));
        result.get(0).assertContentEquals(text);

    }

    @Test
    public void testRdfJsonTransformer() {
        testRunner = TestRunners.newTestRunner(new ProvenanceTransformer());
        testRunner.setProperty(ProvenanceTransformer.OUTPUT_FORMAT, "RDF/JSON");
        testRunner.enqueue(content);
        testRunner.run(1);
        testRunner.assertQueueEmpty();

        List<MockFlowFile> result = testRunner.getFlowFilesForRelationship(ProvenanceTransformer.SUCCESS);
        assertEquals("1 Transformed to Turtle format", 1, result.size());
        result.get(0).assertAttributeEquals("mime.extension", ".rj");
        result.get(0).assertAttributeEquals("mime.type", "application/rdf+json");
        result.get(0).assertContentEquals(readFileAsString("expected-output/expected-output.rj"));
    }

    @Test
    public void testNTriplesTransformer() {
        testRunner = TestRunners.newTestRunner(new ProvenanceTransformer());
        testRunner.setProperty(ProvenanceTransformer.OUTPUT_FORMAT, "N-TRIPLES");
        testRunner.enqueue(content);
        testRunner.run(1);
        testRunner.assertQueueEmpty();

        List<MockFlowFile> result = testRunner.getFlowFilesForRelationship(ProvenanceTransformer.SUCCESS);
        assertEquals("1 Transformed to Turtle format", 1, result.size());
        result.get(0).assertAttributeEquals("mime.extension", ".nt");
        result.get(0).assertAttributeEquals("mime.type", "application/n-triples");
        String text = readFileAsString("expected-output/expected-output.nt");
        text = text.replace("\n", System.getProperty("line.separator"));
        result.get(0).assertContentEquals(text);
    }

}
