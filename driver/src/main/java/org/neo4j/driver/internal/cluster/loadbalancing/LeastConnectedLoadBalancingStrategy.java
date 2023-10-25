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
package org.neo4j.driver.internal.cluster.loadbalancing;

import java.util.List;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;

/**
 * Load balancing strategy that finds server with the least amount of active (checked out of the pool) connections from given readers or writers. It finds a
 * start index for iteration in a round-robin fashion. This is done to prevent choosing same first address over and over when all addresses have the same amount
 * of active connections.
 */
public class LeastConnectedLoadBalancingStrategy implements LoadBalancingStrategy {
    private final RoundRobinArrayIndex readersIndex = new RoundRobinArrayIndex();
    private final RoundRobinArrayIndex writersIndex = new RoundRobinArrayIndex();

    private final ConnectionPool connectionPool;
    private final Logger log;

    public LeastConnectedLoadBalancingStrategy(ConnectionPool connectionPool, Logging logging) {
        this.connectionPool = connectionPool;
        this.log = logging.getLog(getClass());
    }

    @Override
    public BoltServerAddress selectReader(List<BoltServerAddress> knownReaders) {
        return select(knownReaders, readersIndex, "reader");
    }

    @Override
    public BoltServerAddress selectWriter(List<BoltServerAddress> knownWriters) {
        return select(knownWriters, writersIndex, "writer");
    }

    private BoltServerAddress select(
            List<BoltServerAddress> addresses, RoundRobinArrayIndex addressesIndex, String addressType) {
        var size = addresses.size();
        if (size == 0) {
            log.trace("Unable to select %s, no known addresses given", addressType);
            return null;
        }

        // choose start index for iteration in round-robin fashion
        var startIndex = addressesIndex.next(size);
        var index = startIndex;

        BoltServerAddress leastConnectedAddress = null;
        var leastActiveConnections = Integer.MAX_VALUE;

        // iterate over the array to find the least connected address
        do {
            var address = addresses.get(index);
            var activeConnections = connectionPool.inUseConnections(address);

            if (activeConnections < leastActiveConnections) {
                leastConnectedAddress = address;
                leastActiveConnections = activeConnections;
            }

            // loop over to the start of the array when end is reached
            if (index == size - 1) {
                index = 0;
            } else {
                index++;
            }
        } while (index != startIndex);

        log.trace(
                "Selected %s with address: '%s' and active connections: %s",
                addressType, leastConnectedAddress, leastActiveConnections);

        return leastConnectedAddress;
    }
}
