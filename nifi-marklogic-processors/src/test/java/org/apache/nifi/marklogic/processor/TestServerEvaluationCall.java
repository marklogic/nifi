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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.marklogic.client.FailedRequestException;
import com.marklogic.client.ForbiddenUserException;
import com.marklogic.client.Transaction;
import com.marklogic.client.eval.EvalResult;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.io.marker.AbstractReadHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.TextWriteHandle;
import com.marklogic.client.util.EditableNamespaceContext;

public class TestServerEvaluationCall implements ServerEvaluationCall {
    public int xqueryCalls = 0;
    public int javascriptCalls = 0;
    public int modulePathCalls = 0;
    public Map<String, Object> variables = new HashMap<String, Object>();
    @Override
    public ServerEvaluationCall xquery(String xquery) {
        xqueryCalls++;
        return this;
    }

    @Override
    public ServerEvaluationCall xquery(TextWriteHandle xquery) {
        xqueryCalls++;
        return this;
    }

    @Override
    public ServerEvaluationCall javascript(String javascript) {
        javascriptCalls++;
        return this;
    }

    @Override
    public ServerEvaluationCall javascript(TextWriteHandle javascript) {
        javascriptCalls++;
        return this;
    }

    @Override
    public ServerEvaluationCall modulePath(String modulePath) {
        modulePathCalls++;
        return this;
    }

    @Override
    public ServerEvaluationCall addVariable(String name, String value) {
        return addVariableAs(name, (Object) value) ;
    }

    @Override
    public ServerEvaluationCall addVariable(String name, Number value) {
        return addVariableAs(name, (Object) value);
    }

    @Override
    public ServerEvaluationCall addVariable(String name, Boolean value) {
        return addVariableAs(name, (Object) value);
    }

    @Override
    public ServerEvaluationCall addVariable(String name, AbstractWriteHandle value) {
        return addVariableAs(name, (Object) value);
    }

    @Override
    public ServerEvaluationCall addVariableAs(String name, Object value) {
        variables.put(name, value);
        return this;
    }

    @Override
    public ServerEvaluationCall transaction(Transaction transaction) {
        return this;
    }

    @Override
    public ServerEvaluationCall addNamespace(String prefix, String namespaceURI) {
        return this;
    }

    @Override
    public ServerEvaluationCall namespaceContext(EditableNamespaceContext namespaces) {
        return this;
    }

    @Override
    public <T> T evalAs(Class<T> responseType) throws ForbiddenUserException, FailedRequestException {
        return null;
    }

    @Override
    public <H extends AbstractReadHandle> H eval(H responseHandle)
            throws ForbiddenUserException, FailedRequestException {
        return responseHandle;
    }

    @Override
    public EvalResultIterator eval() throws ForbiddenUserException, FailedRequestException {
        return new TestEvalResultIterator();
    }

    public void reset() {
        xqueryCalls = 0;
        javascriptCalls = 0;
        modulePathCalls = 0;
        variables.clear();
    }

    class TestEvalResultIterator implements EvalResultIterator, Iterator<EvalResult> {

        @Override
        public Iterator<EvalResult> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public EvalResult next() {
            return null;
        }

        @Override
        public void close() {
        }
    }
}