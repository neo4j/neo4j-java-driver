/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.testutil;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;

/**
 * A little utility for integration testing, this provides tests with a session they can work with.
 * If you want more direct control, have a look at {@link DatabaseExtension} instead.
 */
public class SessionExtension extends DatabaseExtension implements Session, BeforeEachCallback, AfterEachCallback {
    private Session realSession;

    @Override
    @SuppressWarnings("resource")
    public void beforeEach(ExtensionContext context) throws Exception {
        super.beforeEach(context);
        realSession = driver().session();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (realSession != null) {
            realSession.close();
        }
    }

    @Override
    public boolean isOpen() {
        return realSession.isOpen();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Disallowed on this test session");
    }

    @Override
    public Transaction beginTransaction() {
        return realSession.beginTransaction();
    }

    @Override
    public Transaction beginTransaction(TransactionConfig config) {
        return realSession.beginTransaction(config);
    }

    @Override
    public <T> T executeRead(TransactionCallback<T> callback, TransactionConfig config) {
        return realSession.executeRead(callback, config);
    }

    @Override
    public <T> T executeWrite(TransactionCallback<T> callback, TransactionConfig config) {
        return realSession.executeWrite(callback, config);
    }

    @Override
    public Set<Bookmark> lastBookmarks() {
        return realSession.lastBookmarks();
    }

    @Override
    public Result run(String query, Map<String, Object> parameters) {
        return realSession.run(query, parameters);
    }

    @Override
    public Result run(String query, Value parameters) {
        return realSession.run(query, parameters);
    }

    @Override
    public Result run(String query, Record parameters) {
        return realSession.run(query, parameters);
    }

    @Override
    public Result run(String query) {
        return realSession.run(query);
    }

    @Override
    public Result run(Query query) {
        return realSession.run(query.text(), query.parameters());
    }

    @Override
    public Result run(String query, TransactionConfig config) {
        return realSession.run(query, config);
    }

    @Override
    public Result run(String query, Map<String, Object> parameters, TransactionConfig config) {
        return realSession.run(query, parameters, config);
    }

    @Override
    public Result run(Query query, TransactionConfig config) {
        return realSession.run(query, config);
    }
}
