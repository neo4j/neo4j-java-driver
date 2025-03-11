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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.ListenerEvent;
import org.neo4j.driver.ConnectionPoolMetrics;

class MicrometerMetricsTest {
    static final String ID = "id";

    MicrometerMetrics metrics;
    MeterRegistry registry;
    ConnectionPoolMetrics poolMetrics;
    ConnectionPoolMetricsListener poolMetricsListener;

    @BeforeEach
    void beforeEach() {
        registry = new SimpleMeterRegistry();
        metrics = new MicrometerMetrics(registry);
        poolMetricsListener = mock(
                ConnectionPoolMetricsListener.class,
                Mockito.withSettings().extraInterfaces(ConnectionPoolMetrics.class));
        poolMetrics = (ConnectionPoolMetrics) poolMetricsListener;
    }

    @Test
    void shouldReturnEmptyConnectionPoolMetrics() {
        // GIVEN & WHEN
        var collection = metrics.connectionPoolMetrics();

        // THEN
        assertTrue(collection.isEmpty());
    }

    @Test
    void shouldDelegateBeforeCreating() {
        // GIVEN
        ListenerEvent<?> event = mock(ListenerEvent.class);
        metrics.putPoolMetrics(ID, poolMetrics);

        // WHEN
        metrics.beforeCreating(ID, event);

        // THEN
        assertEquals(1, metrics.connectionPoolMetrics().size());
        then(poolMetricsListener).should().beforeCreating(event);
    }

    @Test
    void shouldDelegateAfterCreated() {
        // GIVEN
        ListenerEvent<?> event = mock(ListenerEvent.class);
        metrics.putPoolMetrics(ID, poolMetrics);

        // WHEN
        metrics.afterCreated(ID, event);

        // THEN
        assertEquals(1, metrics.connectionPoolMetrics().size());
        then(poolMetricsListener).should().afterCreated(event);
    }

    @Test
    void shouldDelegateAfterFailedToCreate() {
        // GIVEN
        metrics.putPoolMetrics(ID, poolMetrics);

        // WHEN
        metrics.afterFailedToCreate(ID);

        // THEN
        assertEquals(1, metrics.connectionPoolMetrics().size());
        then(poolMetricsListener).should().afterFailedToCreate();
    }

    @Test
    void shouldDelegateAfterClosed() {
        // GIVEN
        metrics.putPoolMetrics(ID, poolMetrics);

        // WHEN
        metrics.afterClosed(ID);

        // THEN
        assertEquals(1, metrics.connectionPoolMetrics().size());
        then(poolMetricsListener).should().afterClosed();
    }

    @Test
    void shouldDelegateBeforeAcquiringOrCreating() {
        // GIVEN
        ListenerEvent<?> event = mock(ListenerEvent.class);
        metrics.putPoolMetrics(ID, poolMetrics);

        // WHEN
        metrics.beforeAcquiringOrCreating(ID, event);

        // THEN
        assertEquals(1, metrics.connectionPoolMetrics().size());
        then(poolMetricsListener).should().beforeAcquiringOrCreating(event);
    }

    @Test
    void shouldDelegateAfterAcquiringOrCreating() {
        // GIVEN
        metrics.putPoolMetrics(ID, poolMetrics);

        // WHEN
        metrics.afterAcquiringOrCreating(ID);

        // THEN
        assertEquals(1, metrics.connectionPoolMetrics().size());
        then(poolMetricsListener).should().afterAcquiringOrCreating();
    }

    @Test
    void shouldDelegateAfterAcquiredOrCreated() {
        // GIVEN
        ListenerEvent<?> event = mock(ListenerEvent.class);
        metrics.putPoolMetrics(ID, poolMetrics);

        // WHEN
        metrics.afterAcquiredOrCreated(ID, event);

        // THEN
        assertEquals(1, metrics.connectionPoolMetrics().size());
        then(poolMetricsListener).should().afterAcquiredOrCreated(event);
    }

    @Test
    void shouldDelegateAfterTimedOutToAcquireOrCreate() {
        // GIVEN
        metrics.putPoolMetrics(ID, poolMetrics);

        // WHEN
        metrics.afterTimedOutToAcquireOrCreate(ID);

        // THEN
        assertEquals(1, metrics.connectionPoolMetrics().size());
        then(poolMetricsListener).should().afterTimedOutToAcquireOrCreate();
    }

    @Test
    void shouldDelegateAfterConnectionCreated() {
        // GIVEN
        ListenerEvent<?> event = mock(ListenerEvent.class);
        metrics.putPoolMetrics(ID, poolMetrics);

        // WHEN
        metrics.afterConnectionCreated(ID, event);

        // THEN
        assertEquals(1, metrics.connectionPoolMetrics().size());
        then(poolMetricsListener).should().acquired(event);
    }

    @Test
    void shouldDelegateAfterConnectionReleased() {
        // GIVEN
        ListenerEvent<?> event = mock(ListenerEvent.class);
        metrics.putPoolMetrics(ID, poolMetrics);

        // WHEN
        metrics.afterConnectionReleased(ID, event);

        // THEN
        assertEquals(1, metrics.connectionPoolMetrics().size());
        then(poolMetricsListener).should().released(event);
    }

    @Test
    void shouldCreateListenerEvent() {
        // GIVEN & WHEN
        var event = metrics.createListenerEvent();

        // THEN
        assertTrue(event instanceof MicrometerTimerListenerEvent);
    }

    @Test
    void shouldPutPoolMetrics() {
        // GIVEN
        var size = metrics.connectionPoolMetrics().size();

        // WHEN
        metrics.registerPoolMetrics(ID, BoltServerAddress.LOCAL_DEFAULT, () -> 23, () -> 42);

        // THEN
        assertEquals(size + 1, metrics.connectionPoolMetrics().size());
    }

    @Test
    void shouldRemovePoolMetrics() {
        // GIVEN
        metrics.putPoolMetrics(ID, poolMetrics);
        var size = metrics.connectionPoolMetrics().size();

        // WHEN
        metrics.removePoolMetrics(ID);

        // THEN
        assertEquals(size - 1, metrics.connectionPoolMetrics().size());
    }
}
