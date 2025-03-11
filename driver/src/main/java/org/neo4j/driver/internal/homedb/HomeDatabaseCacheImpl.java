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
package org.neo4j.driver.internal.homedb;

import java.time.Clock;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.neo4j.bolt.connection.BoltConnection;

final class HomeDatabaseCacheImpl implements HomeDatabaseCache {
    private final Map<HomeDatabaseCacheKey, Entry> keyToEntry = new HashMap<>();
    private final Set<BoltConnection> ssrEnabledBoltConnections = new HashSet<>();
    private final Set<BoltConnection> ssrDisabledBoltConnections = new HashSet<>();
    private final int sizeLimit;
    private final int pruneSize;
    private final Clock clock;
    private boolean enabled;

    public HomeDatabaseCacheImpl(int sizeLimit, Clock clock) {
        this.sizeLimit = sizeLimit;
        this.pruneSize = Math.max(
                (int) Math.min(sizeLimit, ((1 / Math.log(Integer.MAX_VALUE)) * 0.8) * sizeLimit * Math.log(sizeLimit)),
                1);
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public synchronized Optional<String> get(HomeDatabaseCacheKey key) {
        return enabled
                ? Optional.ofNullable(keyToEntry.computeIfPresent(
                                key, (ignored, entry) -> new Entry(entry.database(), clock.millis())))
                        .map(Entry::database)
                : Optional.empty();
    }

    @Override
    public synchronized void put(HomeDatabaseCacheKey key, String value) {
        prune();
        keyToEntry.put(key, new Entry(value, -1));
    }

    @Override
    public synchronized void onOpen(BoltConnection boltConnection) {
        if (boltConnection.serverSideRoutingEnabled()) {
            ssrEnabledBoltConnections.add(boltConnection);
        } else {
            ssrDisabledBoltConnections.add(boltConnection);
        }
        updateEnabled();
    }

    @Override
    public synchronized void onClose(BoltConnection boltConnection) {
        if (boltConnection.serverSideRoutingEnabled()) {
            ssrEnabledBoltConnections.remove(boltConnection);
        } else {
            ssrDisabledBoltConnections.remove(boltConnection);
        }
        updateEnabled();
    }

    private synchronized void prune() {
        if (keyToEntry.size() >= sizeLimit) {
            var pruningList = keyToEntry.values().stream()
                    .sorted(Comparator.comparing(Entry::lastUsed))
                    .limit(pruneSize)
                    .toList();
            keyToEntry.values().removeAll(pruningList);
        }
    }

    private synchronized void updateEnabled() {
        enabled = !ssrEnabledBoltConnections.isEmpty() && ssrDisabledBoltConnections.isEmpty();
    }

    synchronized int size() {
        return keyToEntry.size();
    }

    private record Entry(String database, long lastUsed) {}
}
