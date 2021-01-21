package org.apache.nifi.marklogic.processor;

import com.marklogic.client.eval.EvalResult;
import com.marklogic.client.eval.EvalResultIterator;

import java.util.Iterator;
import java.util.List;

public class MockEvalResultIterator implements EvalResultIterator {

	Iterator<EvalResult> iterator;

	public MockEvalResultIterator(List<EvalResult> results) {
		this.iterator = results.iterator();
	}

	@Override
	public Iterator<EvalResult> iterator() {
		return iterator;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public EvalResult next() {
		return iterator.next();
	}

	@Override
	public void close() {
	}
}
