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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;

/**
 * Load balancing strategy which selects N candidates at random from a provided list of known addresses and then chooses
 * the candidate with the least number of active connections (using {@link LeastConnected}
 */
public class NRandomChoicesLoadBalancingStrategy implements LoadBalancingStrategy {

    private final int n;
    private final LeastConnected leastConnected;
    private final Logger log;

    public NRandomChoicesLoadBalancingStrategy(int n, ConnectionPool connectionPool, Logging logging) {
        if (n <= 0) {
            throw new IllegalArgumentException(
                    "Your load balancer must select at least one candidate. n must be >= 1!");
        }
        this.n = n;
        this.leastConnected = new LeastConnected(connectionPool);
        this.log = logging.getLog(getClass());
    }

    @Override
    public BoltServerAddress selectReader(List<BoltServerAddress> knownReaders) {
        return select(knownReaders, "readers");
    }

    @Override
    public BoltServerAddress selectWriter(List<BoltServerAddress> knownWriters) {
        return select(knownWriters, "writers");
    }

    private BoltServerAddress select(List<BoltServerAddress> candidates, String addressType) {
        if (candidates.isEmpty()) {
            log.trace("Unable to select %s, no known addresses given", addressType);
            return null;
        }
        var shuffledCandidates = new ArrayList<>(candidates);
        Collections.shuffle(shuffledCandidates);
        var choice = leastConnected.leastConnected(candidates.subList(0, n));
        choice.ifPresent(c -> log.trace(
                "Selected %s with address: '%s' and active connections: %s",
                addressType, c.address(), c.numInUseConnections()));

        return choice.map(LeastConnected.AddressChoice::address).orElse(null);
    }
}
