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
package org.neo4j.driver.internal.bolt.routedimpl.cluster.loadbalancing;

import java.util.List;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;

/**
 * A facility to select most appropriate reader or writer among the given addresses for request processing.
 */
public interface LoadBalancingStrategy {
    /**
     * Select most appropriate read address from the given array of addresses.
     *
     * @param knownReaders array of all known readers.
     * @return most appropriate reader or {@code null} if it can't be selected.
     */
    BoltServerAddress selectReader(List<BoltServerAddress> knownReaders);

    /**
     * Select most appropriate write address from the given array of addresses.
     *
     * @param knownWriters array of all known writers.
     * @return most appropriate writer or {@code null} if it can't be selected.
     */
    BoltServerAddress selectWriter(List<BoltServerAddress> knownWriters);
}
