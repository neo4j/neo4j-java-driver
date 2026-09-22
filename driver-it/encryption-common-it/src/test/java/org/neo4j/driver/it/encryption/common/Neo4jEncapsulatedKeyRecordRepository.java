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
package org.neo4j.driver.it.encryption.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Driver;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.Record;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Values;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.encryption.EncapsulatedKeyRecord;
import org.neo4j.driver.encryption.async.AsyncEncapsulatedKeyRecordRepository;

final class Neo4jEncapsulatedKeyRecordRepository implements AsyncEncapsulatedKeyRecordRepository {
    private final Driver driver;
    private final SessionConfig sessionConfig;

    Neo4jEncapsulatedKeyRecordRepository(Driver driver, String database) {
        this.driver = Objects.requireNonNull(driver);
        var builder = SessionConfig.builder();
        if (database != null) {
            builder.withDatabase(database);
        }
        this.sessionConfig = builder.build();
    }

    public void createConstraints(boolean legacySyntax) {
        var config = queryConfig();
        if (legacySyntax) {
            driver.executableQuery("""
                    CREATE CONSTRAINT key_id IF NOT EXISTS
                    ON (k:Key)
                    ASSERT k.id IS UNIQUE
                    """).withConfig(config).execute();

            driver.executableQuery("""
                    CREATE CONSTRAINT key_alias IF NOT EXISTS
                    ON (k:Key)
                    ASSERT k.alias IS UNIQUE
                    """).withConfig(config).execute();
        } else {
            driver.executableQuery("""
                    CREATE CONSTRAINT key_id IF NOT EXISTS
                    FOR (k:Key)
                    REQUIRE k.id IS UNIQUE
                    """).withConfig(config).execute();

            driver.executableQuery("""
                    CREATE CONSTRAINT key_alias IF NOT EXISTS
                    FOR (k:Key)
                    REQUIRE k.alias IS UNIQUE
                    """).withConfig(config).execute();
        }
    }

    @Override
    public CompletionStage<EncapsulatedKeyRecord> findByIdAsync(String id) {
        return withSession(session ->
                session.executeReadAsync(tx -> tx.runAsync("MATCH (key:Key {id: $id}) RETURN key", Map.of("id", id))
                        .thenCompose(ResultCursor::nextAsync)
                        .thenApply(this::toKey)));
    }

    @Override
    public CompletionStage<EncapsulatedKeyRecord> findByAliasAsync(String alias) {
        return withSession(session -> session.executeReadAsync(
                tx -> tx.runAsync("MATCH (key:Key {alias: $alias}) RETURN key", Map.of("alias", alias))
                        .thenCompose(ResultCursor::nextAsync)
                        .thenApply(this::toKey)));
    }

    @Override
    public CompletionStage<EncapsulatedKeyRecord> createAsync(
            String alias, byte[] encapsulation, Map<String, String> metadata) {

        var id = UUID.randomUUID().toString();
        var properties = properties(id, alias, encapsulation, metadata);

        return withSession(session -> session.executeWriteAsync(
                tx -> tx.runAsync("CREATE (k:Key $properties)", Map.of("properties", properties))
                        .thenCompose(ResultCursor::consumeAsync)
                        .thenApply(ignored -> EncapsulatedKeyRecord.of(id, alias, encapsulation, metadata))));
    }

    @Override
    public CompletionStage<Void> setAliasByIdAsync(String id, String alias) {
        return withSession(session -> session.executeWriteAsync(tx -> {
            var query = """
                    MATCH (k:Key {id: $id})
                    SET k.alias = $alias
                    """;

            return tx.runAsync(query, Map.of("id", id, "alias", Values.value((Object) alias)))
                    .thenCompose(ResultCursor::consumeAsync)
                    .thenApply(summary -> null);
        }));
    }

    @Override
    public CompletionStage<Void> deleteByIdAsync(String id) {
        return withSession(session ->
                session.executeWriteAsync(tx -> tx.runAsync("MATCH (key:Key {id: $id}) DELETE key", Map.of("id", id))
                        .thenCompose(ResultCursor::consumeAsync)
                        .thenApply(ignored -> null)));
    }

    private Map<String, Object> properties(
            String id, String alias, byte[] encapsulation, Map<String, String> metadata) {

        var properties = new HashMap<String, Object>();
        properties.put("id", id);
        properties.put("alias", alias);
        properties.put("encapsulation", encapsulation);

        metadata.forEach((key, value) -> properties.put("metadata." + key, value));

        return properties;
    }

    private <T> CompletionStage<T> withSession(
            java.util.function.Function<AsyncSession, CompletionStage<T>> operation) {
        var session = driver.session(AsyncSession.class, sessionConfig);
        return operation.apply(session).whenComplete((result, error) -> session.closeAsync());
    }

    private QueryConfig queryConfig() {
        var builder = QueryConfig.builder();
        sessionConfig.database().ifPresent(builder::withDatabase);
        return builder.build();
    }

    private EncapsulatedKeyRecord toKey(Record record) {
        var key = record.get("key");
        var id = key.get("id").asString();
        var aliasValue = key.get("alias");
        var alias = aliasValue.isNull() ? null : aliasValue.asString();
        var encapsulation = key.get("encapsulation").asByteArray();
        var metadata = new HashMap<String, String>();
        for (var field : key.keys()) {
            if (field.startsWith("metadata.")) {
                metadata.put(
                        field.substring("metadata.".length()), key.get(field).asString());
            }
        }
        return EncapsulatedKeyRecord.of(id, alias, encapsulation, Map.copyOf(metadata));
    }
}
