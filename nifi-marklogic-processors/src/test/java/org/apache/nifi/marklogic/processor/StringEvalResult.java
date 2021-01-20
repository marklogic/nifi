package org.apache.nifi.marklogic.processor;

import com.marklogic.client.eval.EvalResult;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractReadHandle;

public class StringEvalResult implements EvalResult {

	private String value;

	public StringEvalResult(String value) {
		this.value = value;
	}

	@Override
	public Type getType() {
		return Type.STRING;
	}

	@Override
	public Format getFormat() {
		return Format.TEXT;
	}

	@Override
	public <H extends AbstractReadHandle> H get(H h) {
		return (H)new StringHandle(value);
	}

	@Override
	public <T> T getAs(Class<T> aClass) {
		return (T)String.class;
	}

	@Override
	public String getString() {
		return value;
	}

	@Override
	public Number getNumber() {
		return null;
	}

	@Override
	public Boolean getBoolean() {
		return null;
	}
}
