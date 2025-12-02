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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

final class InMemoryEntryCache<T> implements Cache<T> {
    private final Map<String, Entry<T>> keyToEntry;
    private final int sizeLimit;
    private final Duration ttl;

    InMemoryEntryCache(int sizeLimit, Duration ttl) {
        if (sizeLimit <= 0) {
            throw new IllegalArgumentException("sizeLimit must be greater than zero");
        }

        this.sizeLimit = sizeLimit;
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.keyToEntry = new HashMap<>();

        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("ttl must be greater than zero");
        }
    }

    @Override
    public T get(String key, long now) {
        Objects.requireNonNull(key);

        var entry = keyToEntry.get(key);
        if (entry == null) {
            return null;
        }

        if (entry.isExpired(now, ttl)) {
            keyToEntry.remove(key);
            return null;
        }

        entry.accessedAt(now);
        return entry.value();
    }

    @Override
    public void put(String key, T value, long now) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        var leastRecentlyUsedKey = prune(now);

        if (keyToEntry.size() >= sizeLimit && !keyToEntry.containsKey(key) && leastRecentlyUsedKey != null) {
            keyToEntry.remove(leastRecentlyUsedKey);
        }

        keyToEntry.put(key, new Entry<>(value, now, now));
    }

    @Override
    public void delete(String key) {
        Objects.requireNonNull(key);
        keyToEntry.remove(key);
    }

    private String prune(long now) {
        String leastRecentlyUsedKey = null;
        var leastRecentlyUsedKeyAccessedAt = Long.MAX_VALUE;

        var iterator = keyToEntry.entrySet().iterator();

        while (iterator.hasNext()) {
            var entry = iterator.next();
            var value = entry.getValue();

            if (value.isExpired(now, ttl)) {
                iterator.remove();
                continue;
            }

            if (value.lastAccessAt() < leastRecentlyUsedKeyAccessedAt) {
                leastRecentlyUsedKey = entry.getKey();
                leastRecentlyUsedKeyAccessedAt = value.lastAccessAt();
            }
        }

        return leastRecentlyUsedKey;
    }

    private static class Entry<T> {
        private final T value;
        private final long createdAt;
        private long lastAccessAt;

        Entry(T value, long createdAt, long lastAccessAt) {
            this.value = value;
            this.createdAt = createdAt;
            this.lastAccessAt = lastAccessAt;
        }

        T value() {
            return value;
        }

        long lastAccessAt() {
            return lastAccessAt;
        }

        void accessedAt(long now) {
            lastAccessAt = now;
        }

        boolean isExpired(long now, Duration ttl) {
            if (now < createdAt) {
                return true;
            }

            return Duration.ofMillis(now).minusMillis(createdAt).compareTo(ttl) >= 0;
        }
    }
}
