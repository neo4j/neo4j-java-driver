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
package org.neo4j.driver.internal.bolt.routedimpl.cluster;

import java.util.Optional;
import java.util.Set;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.ClusterComposition;

public class ClusterCompositionLookupResult {
    private final ClusterComposition composition;

    private final Set<BoltServerAddress> resolvedInitialRouters;

    public ClusterCompositionLookupResult(ClusterComposition composition) {
        this(composition, null);
    }

    public ClusterCompositionLookupResult(
            ClusterComposition composition, Set<BoltServerAddress> resolvedInitialRouters) {
        this.composition = composition;
        this.resolvedInitialRouters = resolvedInitialRouters;
    }

    public ClusterComposition getClusterComposition() {
        return composition;
    }

    public Optional<Set<BoltServerAddress>> getResolvedInitialRouters() {
        return Optional.ofNullable(resolvedInitialRouters);
    }
}
