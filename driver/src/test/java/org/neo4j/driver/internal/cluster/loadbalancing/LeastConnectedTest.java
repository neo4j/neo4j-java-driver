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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;

class LeastConnectedTest {

    @Test
    void shouldSelectTheAddressWithTheLeastActiveConnections() {
        // GIVEN
        var boltAddressA = new BoltServerAddress("serverA:1");
        var boltAddressB = new BoltServerAddress("serverB:1");
        var connectionPool = mock(ConnectionPool.class);
        when(connectionPool.inUseConnections(boltAddressA)).thenReturn(5);
        when(connectionPool.inUseConnections(boltAddressB)).thenReturn(2);
        var leastConnected = new LeastConnected(connectionPool);

        // WHEN
        var selected = leastConnected.leastConnected(List.of(boltAddressA, boltAddressB));

        // THEN
        assertEquals(selected, boltAddressB);
    }
}
