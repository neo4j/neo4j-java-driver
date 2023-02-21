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
package org.neo4j.driver.internal.cluster.loadbalancing;

import static java.util.function.BinaryOperator.minBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Optional;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;

/**
 * Helper class provides the {@link BoltServerAddress} from a provided list with the smallest number of active
 * connections, if one exists.
 *
 * Note that if many addresses have the same smallest number of active connections, the address returned is random.
 */
class LeastConnected {
    private final ConnectionPool connectionPool;
    private final Comparator<AddressChoice> comparingNumberOfConnections;

    LeastConnected(ConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
        this.comparingNumberOfConnections = Comparator.comparing(AddressChoice::numInUseConnections);
    }

    Optional<AddressChoice> leastConnected(Collection<BoltServerAddress> candidates) {
        var shuffledCandidates = new ArrayList<>(candidates);
        Collections.shuffle(shuffledCandidates);
        return shuffledCandidates.stream()
                .map(address -> new AddressChoice(address, connectionPool.inUseConnections(address)))
                .reduce(minBy(comparingNumberOfConnections));
    }

    record AddressChoice(BoltServerAddress address, int numInUseConnections) {}
}
