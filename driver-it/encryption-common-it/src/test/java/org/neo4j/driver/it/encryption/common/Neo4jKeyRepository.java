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
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.property_encryption.PropertyEncryptionProfile;
import org.neo4j.driver.types.TypeSystem;

final class Neo4jKeyRepository implements PropertyEncryptionProfile.Envelope.EncapsulatedKeyRepository {
    private final Driver driver;
    private final String database;

    Neo4jKeyRepository(Driver driver, String database) {
        this.driver = Objects.requireNonNull(driver);
        this.database = database;
    }

    public void createConstraints() {
        var builder = QueryConfig.builder();
        if (database != null) {
            builder.withDatabase(database);
        }
        var config = builder.build();
        driver.executableQuery("""
                CREATE CONSTRAINT key_id IF NOT EXISTS
                FOR (k:Key)
                REQUIRE k.id IS UNIQUE
                """).withConfig(config).execute();
        driver.executableQuery("""
                CREATE CONSTRAINT key_aliases IF NOT EXISTS
                FOR (k:Key)
                REQUIRE k.aliases IS UNIQUE
                """).withConfig(config).execute();
    }

    @Override
    public CompletionStage<EncapsulatedKey> findById(String id) {
        return session().executeReadAsync(tx -> tx.runAsync("MATCH (key:Key {id: $id}) RETURN key", Map.of("id", id))
                .thenCompose(resultCursor -> resultCursor.nextAsync().thenApply(this::toKey)));
    }

    @Override
    public CompletionStage<EncapsulatedKey> findByAlias(String alias) {
        return session().executeReadAsync(tx -> tx.runAsync(
                        "MATCH (key:Key) WHERE $alias IN key.aliases RETURN key", Map.of("alias", alias))
                .thenCompose(resultCursor -> resultCursor.nextAsync().thenApply(this::toKey)));
    }

    @Override
    public CompletionStage<EncapsulatedKey> save(String alias, byte[] encapsulation, Map<String, String> metadata) {
        var id = UUID.randomUUID().toString();
        var properties = new HashMap<String, Object>();
        properties.put("id", id);
        properties.put("alias", alias);
        properties.put("encapsulation", encapsulation);
        for (var entry : metadata.entrySet()) {
            properties.put("metadata." + entry.getKey(), entry.getValue());
        }

        return session().executeWriteAsync(tx -> tx.runAsync(
                        "MERGE (k:Key {id: $properties.id}) SET k = $properties", Map.of("properties", properties))
                .thenCompose(ResultCursor::consumeAsync)
                .thenApply(summary -> EncapsulatedKey.of(id, alias, encapsulation, metadata)));
    }

    @Override
    public CompletionStage<Void> updateAliasById(String id, String alias) {
        return session().executeWriteAsync(tx -> tx.runAsync("MATCH (key:Key {id: $id}) RETURN key", Map.of("id", id))
                .thenCompose(resultCursor -> resultCursor.nextAsync().thenCompose(record -> {
                    if (record == null) {
                        throw new ClientException("No key found");
                    } else {
                        var key = toKey(record);
                        if (alias == null) {
                            return tx.runAsync("MERGE (k:Key {id: $id}) REMOVE k.alias", Map.of("id", id))
                                    .thenCompose(ResultCursor::consumeAsync)
                                    .thenApply(summary -> null);
                        } else {
                            return tx.runAsync(
                                            "MERGE (k:Key {id: $id}) SET k.alias = $alias",
                                            Map.of("id", id, "alias", alias))
                                    .thenCompose(ResultCursor::consumeAsync)
                                    .thenApply(summary -> null);
                        }
                    }
                })));
    }

    @Override
    public CompletionStage<Void> deleteById(String id) {
        return session().executeWriteAsync(tx -> tx.runAsync("MATCH (key:Key {id: $id}) DELETE key", Map.of("id", id))
                .thenCompose(ResultCursor::consumeAsync)
                .thenApply(ignored -> null));
    }

    private AsyncSession session() {
        var builder = SessionConfig.builder();
        if (database != null) {
            builder.withDatabase(database);
        }
        return driver.session(AsyncSession.class, builder.build());
    }

    private EncapsulatedKey toKey(Record record) {
        var key = record.get("key");
        var id = key.get("id").asString();
        var aliasValue = key.get("alias");
        var alias = aliasValue != null && TypeSystem.getDefault().STRING().isTypeOf(aliasValue)
                ? aliasValue.asString()
                : null;
        var encapsulation = key.get("encapsulation").asByteArray();
        var managerMetadata = new HashMap<String, String>();
        for (var field : key.keys()) {
            if (field.startsWith("metadata.")) {
                managerMetadata.put(
                        field.replace("metadata.", ""), key.get(field).asString());
            }
        }
        return EncapsulatedKey.of(id, alias, encapsulation, Map.copyOf(managerMetadata));
    }
}
