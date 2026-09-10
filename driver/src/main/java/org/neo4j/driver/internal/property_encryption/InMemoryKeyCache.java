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
package org.neo4j.driver.internal.property_encryption;

import java.time.Clock;
import javax.crypto.SecretKey;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.property_encryption.CacheConfig;

final class InMemoryKeyCache implements KeyCache {
    private final Cache<String> aliasIndex;
    private final Cache<SecretKey> keyCache;
    private final Clock clock;

    InMemoryKeyCache(CacheConfig aliasCacheConfig, CacheConfig keyCacheConfig, Clock clock) {
        this.aliasIndex = aliasCacheConfig != null
                ? new InMemoryEntryCache<>(aliasCacheConfig.maxSize(), aliasCacheConfig.ttl())
                : new NoopCache<>();
        this.keyCache = keyCacheConfig != null
                ? new InMemoryEntryCache<>(keyCacheConfig.maxSize(), keyCacheConfig.ttl())
                : new NoopCache<>();
        this.clock = clock;
    }

    @Override
    public synchronized void create(String id, String alias, SecretKey key) {
        var now = clock.millis();

        var existingKey = keyCache.get(id, now);
        if (existingKey != null) {
            if (!existingKey.equals(key)) {
                throw new ClientException("Key id=%s collision, make sure key ids are globally unique", id);
            }
        } else {
            keyCache.put(id, key, now);
        }

        if (alias != null) {
            aliasIndex.put(alias, id, now);
        }
    }

    @Override
    public synchronized KeyData findById(String id) {
        var key = keyCache.get(id, clock.millis());
        return key != null ? new KeyData(id, key) : null;
    }

    @Override
    public synchronized KeyData findByAlias(String alias) {
        var now = clock.millis();

        var id = aliasIndex.get(alias, now);

        if (id == null) {
            return null;
        }

        var key = keyCache.get(id, now);
        if (key == null) {
            // key is not cached, purge alias
            aliasIndex.delete(alias);
        }

        return key != null ? new KeyData(id, key) : null;
    }

    @Override
    public synchronized void setAlias(String id, String newAlias) {
        var now = clock.millis();

        if (newAlias != null) {
            aliasIndex.put(newAlias, id, now);
        } else {
            aliasIndex.delete(id);
        }
    }

    @Override
    public synchronized void deleteById(String id) {
        keyCache.delete(id);
    }
}
