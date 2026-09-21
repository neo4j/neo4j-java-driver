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
package neo4j.org.testkit.backend;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.encryption.EncapsulatedKeyRecord;
import org.neo4j.driver.encryption.EncapsulatedKeyRecordRepository;

public final class InMemoryEncapsulatedKeyRecordRepository implements EncapsulatedKeyRecordRepository {
    private final Map<String, Key> idToKey = new HashMap<>();
    private final Map<String, String> aliasToId = new HashMap<>();

    @Override
    public CompletionStage<EncapsulatedKeyRecord> findById(String id) {
        var key = idToKey.get(id);
        if (key == null) {
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.completedFuture(toRecord(id, key));
    }

    @Override
    public CompletionStage<EncapsulatedKeyRecord> findByAlias(String alias) {
        if (alias == null) {
            return CompletableFuture.completedFuture(null);
        }

        var id = aliasToId.get(alias);
        if (id == null) {
            return CompletableFuture.completedFuture(null);
        }

        return findById(id);
    }

    @Override
    public CompletionStage<EncapsulatedKeyRecord> create(
            String alias, byte[] encapsulation, Map<String, String> metadata) {
        var id = UUID.randomUUID().toString();
        save(id, alias, encapsulation, metadata);
        return findById(id);
    }

    @Override
    public CompletionStage<Void> setAliasById(String id, String alias) {
        var key = idToKey.get(id);
        if (key == null) {
            throw new IllegalArgumentException("No key exists with id: " + id);
        }

        if (key.alias() != null) {
            aliasToId.remove(key.alias());
        }

        if (alias == null) {
            idToKey.put(id, key.withAlias(null));
            return CompletableFuture.completedFuture(null);
        }

        var existingId = aliasToId.get(alias);
        if (existingId != null && !existingId.equals(id)) {
            throw new IllegalArgumentException("Alias is already assigned to another key: " + alias);
        }

        aliasToId.put(alias, id);
        idToKey.put(id, key.withAlias(alias));

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> deleteById(String id) {
        var key = idToKey.remove(id);
        if (key != null && key.alias() != null) {
            aliasToId.remove(key.alias());
        }

        return CompletableFuture.completedFuture(null);
    }

    public CompletionStage<EncapsulatedKeyRecord> save(
            String id, String alias, byte[] encapsulation, Map<String, String> metadata) {
        if (alias != null) {
            var existingId = aliasToId.get(alias);
            if (existingId != null && !existingId.equals(id)) {
                throw new IllegalArgumentException("Alias is already assigned to another key: " + alias);
            }
            aliasToId.put(alias, id);
        }

        idToKey.put(id, new Key(alias, encapsulation, metadata));

        return CompletableFuture.completedFuture(toRecord(id, idToKey.get(id)));
    }

    private EncapsulatedKeyRecord toRecord(String id, Key key) {
        return EncapsulatedKeyRecord.of(id, key.alias(), key.encapsulation(), key.metadata());
    }

    private record Key(String alias, byte[] encapsulation, Map<String, String> metadata) {
        private Key withAlias(String alias) {
            return new Key(alias, encapsulation, metadata);
        }
    }
}
