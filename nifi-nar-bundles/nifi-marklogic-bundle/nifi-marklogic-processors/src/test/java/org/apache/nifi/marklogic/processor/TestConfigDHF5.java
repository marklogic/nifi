package org.apache.nifi.marklogic.processor;

import com.marklogic.junit5.spring.SimpleTestConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * SimpleTestConfig looks for mlTestRestPort by default, but we only have mlRestPort. So this overrides it to
 * use mlRestPort instead as the REST port number.
 */
@Configuration
@PropertySource(
	value = {"file:gradle.properties", "file:gradle-local.properties"},
	ignoreResourceNotFound = true
)
public class TestConfigDHF5 extends SimpleTestConfig {

	@Value("${mlRestPortDHF5:0}")
	private Integer restPort;

	@Value("${mlDatabaseDHF5:0}")	
	private String database;
	
	@Override
	public Integer getRestPort() {
		return restPort;
	}
	
	public String getDatabase() {
		return database;
	}
}
