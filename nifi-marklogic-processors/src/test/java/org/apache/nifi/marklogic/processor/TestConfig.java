package org.apache.nifi.marklogic.processor;

import com.marklogic.junit5.spring.SimpleTestConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * SimpleTestConfig looks for mlTestRestPort by default, but we only have mlRestPort. So this overrides it to
 * use mlRestPort instead as the REST port number.
 */
@Configuration
@PropertySource(
    value = {"classpath:test.properties"},
    ignoreResourceNotFound = true
)
public class TestConfig extends SimpleTestConfig {

    @Override
    public Integer getRestPort() {
        return 8006;
    }
}
