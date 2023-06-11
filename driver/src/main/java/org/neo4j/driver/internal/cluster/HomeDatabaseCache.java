/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package org.neo4j.driver.internal.cluster;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.driver.AuthToken;

public class HomeDatabaseCache {
    private final Map<Object, NameAndTtl> keyToDatabase = new ConcurrentHashMap<>();
    private final TemporalAmount maxHomeDatabaseDelay;
    private final Clock clock;

    public HomeDatabaseCache(long delayMillis, Clock clock) {
        this.maxHomeDatabaseDelay = Duration.of(delayMillis, ChronoUnit.MILLIS);
        this.clock = clock;
    }

    public Optional<String> getName(String impersonatedUser, AuthToken authToken) {
        return impersonatedUser != null
                ? getNameInternal(impersonatedUser)
                : authToken != null ? getNameInternal(authToken) : getNameInternal(this);
    }

    public void put(String impersonatedUser, AuthToken authToken, String name) {
        putInternal(
                Objects.requireNonNullElseGet(impersonatedUser, () -> Objects.requireNonNullElse(authToken, this)),
                name);
    }

    public void purge() {
        keyToDatabase.clear();
    }

    private Optional<String> getNameInternal(Object key) {
        var nameAndTtl = keyToDatabase.get(key);
        return nameAndTtl != null && nameAndTtl.expiration().isAfter(Instant.now(clock))
                ? Optional.of(nameAndTtl.name())
                : Optional.empty();
    }

    private void putInternal(Object key, String name) {
        keyToDatabase.put(key, new NameAndTtl(name, Instant.now(clock).plus(maxHomeDatabaseDelay)));
    }

    private record NameAndTtl(String name, Instant expiration) {}
}
