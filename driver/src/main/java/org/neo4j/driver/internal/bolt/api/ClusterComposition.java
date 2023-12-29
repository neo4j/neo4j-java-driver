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
package org.neo4j.driver.internal.bolt.api;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.neo4j.driver.Value;

public final class ClusterComposition {
    private static final long MAX_TTL = Long.MAX_VALUE / 1000L;
    private static final Function<Value, BoltServerAddress> OF_BoltServerAddress =
            value -> new BoltServerAddress(value.asString());

    private final Set<BoltServerAddress> readers;
    private final Set<BoltServerAddress> writers;
    private final Set<BoltServerAddress> routers;
    private final long expirationTimestamp;
    private final String databaseName;

    private ClusterComposition(long expirationTimestamp, String databaseName) {
        this.readers = new LinkedHashSet<>();
        this.writers = new LinkedHashSet<>();
        this.routers = new LinkedHashSet<>();
        this.expirationTimestamp = expirationTimestamp;
        this.databaseName = databaseName;
    }

    public ClusterComposition(
            long expirationTimestamp,
            Set<BoltServerAddress> readers,
            Set<BoltServerAddress> writers,
            Set<BoltServerAddress> routers,
            String databaseName) {
        this(expirationTimestamp, databaseName);
        this.readers.addAll(readers);
        this.writers.addAll(writers);
        this.routers.addAll(routers);
    }

    public boolean hasWriters() {
        return !writers.isEmpty();
    }

    public boolean hasRoutersAndReaders() {
        return !routers.isEmpty() && !readers.isEmpty();
    }

    public Set<BoltServerAddress> readers() {
        return new LinkedHashSet<>(readers);
    }

    public Set<BoltServerAddress> writers() {
        return new LinkedHashSet<>(writers);
    }

    public Set<BoltServerAddress> routers() {
        return new LinkedHashSet<>(routers);
    }

    public long expirationTimestamp() {
        return this.expirationTimestamp;
    }

    public String databaseName() {
        return databaseName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (ClusterComposition) o;
        return expirationTimestamp == that.expirationTimestamp
                && Objects.equals(databaseName, that.databaseName)
                && Objects.equals(readers, that.readers)
                && Objects.equals(writers, that.writers)
                && Objects.equals(routers, that.routers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readers, writers, routers, expirationTimestamp, databaseName);
    }

    @Override
    public String toString() {
        return "ClusterComposition{" + "readers="
                + readers + ", writers="
                + writers + ", routers="
                + routers + ", expirationTimestamp="
                + expirationTimestamp + ", databaseName="
                + databaseName + '}';
    }

    private Set<BoltServerAddress> servers(String role) {
        return switch (role) {
            case "READ" -> readers;
            case "WRITE" -> writers;
            case "ROUTE" -> routers;
            default -> throw new IllegalArgumentException("invalid server role: " + role);
        };
    }
}
