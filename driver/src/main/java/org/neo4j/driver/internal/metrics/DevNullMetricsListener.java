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
package org.neo4j.driver.internal.metrics;

import java.util.function.IntSupplier;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.ListenerEvent;
import org.neo4j.bolt.connection.MetricsListener;

public enum DevNullMetricsListener implements MetricsListener {
    INSTANCE;

    @Override
    public void beforeCreating(String poolId, ListenerEvent<?> creatingEvent) {}

    @Override
    public void afterCreated(String poolId, ListenerEvent<?> creatingEvent) {}

    @Override
    public void afterFailedToCreate(String poolId) {}

    @Override
    public void afterClosed(String poolId) {}

    @Override
    public void beforeAcquiringOrCreating(String poolId, ListenerEvent<?> acquireEvent) {}

    @Override
    public void afterAcquiringOrCreating(String poolId) {}

    @Override
    public void afterAcquiredOrCreated(String poolId, ListenerEvent<?> acquireEvent) {}

    @Override
    public void afterTimedOutToAcquireOrCreate(String poolId) {}

    @Override
    public void afterConnectionCreated(String poolId, ListenerEvent<?> inUseEvent) {}

    @Override
    public void afterConnectionReleased(String poolId, ListenerEvent<?> inUseEvent) {}

    @Override
    public ListenerEvent<?> createListenerEvent() {
        return DevNullListenerEvent.INSTANCE;
    }

    @Override
    public void registerPoolMetrics(
            String poolId, BoltServerAddress serverAddress, IntSupplier inUseSupplier, IntSupplier idleSupplier) {}

    @Override
    public void removePoolMetrics(String poolId) {}

    @Override
    public String toString() {
        return "Driver metrics are not available if they are not enabled.";
    }
}
