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
package org.neo4j.driver.encryption.azure_keyvault;

import com.azure.security.keyvault.keys.cryptography.CryptographyAsyncClient;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

final class CryptographyClientCache {
    private final int maxSize;
    private final Map<String, CacheEntry> clients = new HashMap<>();

    CryptographyClientCache(int maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be greater than zero");
        }
        this.maxSize = maxSize;
    }

    synchronized CryptographyAsyncClient get(String keyId, Function<String, CryptographyAsyncClient> factory) {
        var entry = clients.get(keyId);

        if (entry != null) {
            return entry.touch();
        }

        var client = factory.apply(keyId);
        clients.put(keyId, new CacheEntry(client));

        if (clients.size() > maxSize) {
            evictLeastRecentlyUsed();
        }

        return client;
    }

    private void evictLeastRecentlyUsed() {
        var lru = clients.entrySet().stream()
                .min(Comparator.comparingLong(entry -> entry.getValue().lastUsedNanos))
                .orElseThrow();

        clients.remove(lru.getKey());
    }

    private static final class CacheEntry {
        private final CryptographyAsyncClient client;
        private long lastUsedNanos;

        private CacheEntry(CryptographyAsyncClient client) {
            this.client = client;
            this.lastUsedNanos = System.nanoTime();
        }

        private CryptographyAsyncClient touch() {
            lastUsedNanos = System.nanoTime();
            return client;
        }
    }
}
