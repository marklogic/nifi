package org.apache.nifi.marklogic.processor;

import org.springframework.util.FileCopyUtils;

import java.io.IOException;

abstract class AbstractProvenanceTest {

	ClassLoader classLoader;

	String readFileAsString(String path) {
		return new String(readFileAsByteArray(path));
	}

	private byte[] readFileAsByteArray(String path) {
		try {
			return FileCopyUtils.copyToByteArray(classLoader.getResourceAsStream(path));
		} catch (IOException e) {
			throw new RuntimeException("Unable to read file from path: " + path, e);
		}
	}
}
