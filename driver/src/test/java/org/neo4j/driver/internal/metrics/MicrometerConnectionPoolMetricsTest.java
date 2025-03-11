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
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.ListenerEvent;
import org.neo4j.driver.ConnectionPoolMetrics;

class MicrometerConnectionPoolMetricsTest {
    static final String ID = "id";

    MicrometerConnectionPoolMetrics metrics;
    BoltServerAddress address;
    MeterRegistry registry;
    final AtomicInteger inUse = new AtomicInteger(0);
    final IntSupplier inUseSupplier = inUse::get;
    final AtomicInteger idle = new AtomicInteger(0);
    final IntSupplier idleSupplier = idle::get;

    @BeforeEach
    void beforeEach() {
        address = new BoltServerAddress("host", "127.0.0.1", 7687);
        registry = new SimpleMeterRegistry();
        metrics = new MicrometerConnectionPoolMetrics(ID, address, inUseSupplier, idleSupplier, registry);
    }

    @Test
    void shouldIncrementCreatingAndStartTimerOnBeforeCreating() {
        // GIVEN
        var expectedMetrics = mock(ConnectionPoolMetrics.class);
        given(expectedMetrics.creating()).willReturn(1);
        ListenerEvent<?> event = mock(ListenerEvent.class);

        // WHEN
        metrics.beforeCreating(event);

        // THEN
        verifyMetrics(expectedMetrics, metrics);
        then(event).should().start();
    }

    @Test
    void shouldIncrementFailedToCreateAndDecrementCreatingOnAfterFailedToCreate() {
        // GIVEN
        var expectedMetrics = mock(ConnectionPoolMetrics.class);
        given(expectedMetrics.failedToCreate()).willReturn(1L);
        given(expectedMetrics.creating()).willReturn(-1);

        // WHEN
        metrics.afterFailedToCreate();

        // THEN
        verifyMetrics(expectedMetrics, metrics);
    }

    @Test
    void shouldDecrementCreatingAndIncrementCreatedAndStopTimerOnAfterCreated() {
        // GIVEN
        var expectedMetrics = mock(ConnectionPoolMetrics.class);
        given(expectedMetrics.creating()).willReturn(-1);
        given(expectedMetrics.created()).willReturn(1L);
        var timer = registry.get(MicrometerConnectionPoolMetrics.CREATION).timer();
        var timerCount = timer.count();
        var event = new MicrometerTimerListenerEvent(registry);
        event.start();

        // WHEN
        metrics.afterCreated(event);

        // THEN
        verifyMetrics(expectedMetrics, metrics);
        assertEquals(timerCount + 1, timer.count());
    }

    @Test
    void shouldIncrementClosedOnAfterClosed() {
        // GIVEN
        var expectedMetrics = mock(ConnectionPoolMetrics.class);
        given(expectedMetrics.closed()).willReturn(1L);

        // WHEN
        metrics.afterClosed();

        // THEN
        verifyMetrics(expectedMetrics, metrics);
    }

    @Test
    void shouldStartTimerAndIncrementAcquiringOnBeforeAcquiringOrCreating() {
        // GIVEN
        ListenerEvent<?> event = mock(ListenerEvent.class);
        var expectedMetrics = mock(ConnectionPoolMetrics.class);
        given(expectedMetrics.acquiring()).willReturn(1);

        // WHEN
        metrics.beforeAcquiringOrCreating(event);

        // THEN
        then(event).should().start();
        verifyMetrics(expectedMetrics, metrics);
    }

    @Test
    void shouldDecrementAcquiringOnAfterAcquiringOrCreating() {
        // GIVEN
        var expectedMetrics = mock(ConnectionPoolMetrics.class);
        given(expectedMetrics.acquiring()).willReturn(-1);

        // WHEN
        metrics.afterAcquiringOrCreating();

        // THEN
        verifyMetrics(expectedMetrics, metrics);
    }

    @Test
    void shouldIncrementAcquiredAndStopTimerOnAfterAcquiredOrCreated() {
        // GIVEN
        var expectedMetrics = mock(ConnectionPoolMetrics.class);
        given(expectedMetrics.acquired()).willReturn(1L);
        var timer = registry.get(MicrometerConnectionPoolMetrics.ACQUISITION).timer();
        var timerCount = timer.count();
        var event = new MicrometerTimerListenerEvent(registry);
        event.start();

        // WHEN
        metrics.afterAcquiredOrCreated(event);

        // THEN
        verifyMetrics(expectedMetrics, metrics);
        assertEquals(timerCount + 1, timer.count());
    }

    @Test
    void shouldIncrementTimedOutToAcquireOnAfterTimedOutToAcquireOrCreate() {
        // GIVEN
        var expectedMetrics = mock(ConnectionPoolMetrics.class);
        given(expectedMetrics.timedOutToAcquire()).willReturn(1L);

        // WHEN
        metrics.afterTimedOutToAcquireOrCreate();

        // THEN
        verifyMetrics(expectedMetrics, metrics);
    }

    @Test
    void shouldStartTimerOnAcquired() {
        // GIVEN
        ListenerEvent<?> event = mock(ListenerEvent.class);

        // WHEN
        metrics.acquired(event);

        // THEN
        then(event).should().start();
    }

    @Test
    void shouldIncrementReleasedAndStopTimerOnReleased() {
        // GIVEN
        var expectedMetrics = mock(ConnectionPoolMetrics.class);
        given(expectedMetrics.totalInUseCount()).willReturn(1L);
        var timer = registry.get(MicrometerConnectionPoolMetrics.USAGE).timer();
        var timerCount = timer.count();
        var event = new MicrometerTimerListenerEvent(registry);
        event.start();

        // WHEN
        metrics.released(event);

        // THEN
        verifyMetrics(expectedMetrics, metrics);
        assertEquals(timerCount + 1, timer.count());
    }

    @Test
    void shouldUseInUseSupplier() {
        try {
            // GIVEN
            var expected = 5;
            inUse.compareAndSet(0, expected);
            var expectedMetrics = mock(ConnectionPoolMetrics.class);
            given(expectedMetrics.inUse()).willReturn(expected);

            // WHEN
            var actual = metrics.inUse();

            // THEN
            assertEquals(expected, actual);
            verifyMetrics(expectedMetrics, metrics);
        } finally {
            inUse.set(0);
        }
    }

    @Test
    void shouldUseIdleSupplier() {
        try {
            // GIVEN
            var expected = 5;
            idle.compareAndSet(0, expected);
            var expectedMetrics = mock(ConnectionPoolMetrics.class);
            given(expectedMetrics.idle()).willReturn(expected);

            // WHEN
            var actual = metrics.idle();

            // THEN
            assertEquals(expected, actual);
            verifyMetrics(expectedMetrics, metrics);
        } finally {
            idle.set(0);
        }
    }

    void verifyMetrics(ConnectionPoolMetrics expected, ConnectionPoolMetrics actual) {
        assertEquals(ID, actual.id());
        assertEquals(expected.inUse(), actual.inUse());
        assertEquals(
                expected.inUse(),
                registry.get(MicrometerConnectionPoolMetrics.IN_USE).gauge().value());
        assertEquals(expected.idle(), actual.idle());
        assertEquals(
                expected.idle(),
                registry.get(MicrometerConnectionPoolMetrics.IDLE).gauge().value());
        assertEquals(expected.creating(), actual.creating());
        assertEquals(
                expected.creating(),
                registry.get(MicrometerConnectionPoolMetrics.CREATING).gauge().value());
        assertEquals(expected.created(), actual.created());
        assertEquals(
                expected.created(),
                registry.get(MicrometerConnectionPoolMetrics.CREATION).timer().count());
        assertEquals(expected.failedToCreate(), actual.failedToCreate());
        assertEquals(
                expected.failedToCreate(),
                registry.get(MicrometerConnectionPoolMetrics.FAILED).counter().count());
        assertEquals(expected.closed(), actual.closed());
        assertEquals(
                expected.closed(),
                registry.get(MicrometerConnectionPoolMetrics.CLOSED).counter().count());
        assertEquals(expected.acquiring(), actual.acquiring());
        assertEquals(
                expected.acquiring(),
                registry.get(MicrometerConnectionPoolMetrics.ACQUIRING).gauge().value());
        assertEquals(expected.acquired(), actual.acquired());
        assertEquals(
                expected.acquired(),
                registry.get(MicrometerConnectionPoolMetrics.ACQUISITION)
                        .timer()
                        .count());
        assertEquals(expected.timedOutToAcquire(), actual.timedOutToAcquire());
        assertEquals(
                expected.timedOutToAcquire(),
                registry.get(MicrometerConnectionPoolMetrics.ACQUISITION_TIMEOUT)
                        .counter()
                        .count());
        assertEquals(expected.totalAcquisitionTime(), actual.totalAcquisitionTime());
        assertEquals(expected.totalAcquisitionTime(), (long) registry.get(MicrometerConnectionPoolMetrics.ACQUISITION)
                .timer()
                .totalTime(TimeUnit.MILLISECONDS));
        assertEquals(expected.totalConnectionTime(), actual.totalConnectionTime());
        assertEquals(expected.totalConnectionTime(), (long)
                registry.get(MicrometerConnectionPoolMetrics.CREATION).timer().totalTime(TimeUnit.MILLISECONDS));
        assertEquals(expected.totalInUseTime(), actual.totalInUseTime());
        assertEquals(expected.totalInUseTime(), (long)
                registry.get(MicrometerConnectionPoolMetrics.USAGE).timer().totalTime(TimeUnit.MILLISECONDS));
        assertEquals(expected.totalInUseCount(), actual.totalInUseCount());
        assertEquals(
                expected.totalInUseCount(),
                registry.get(MicrometerConnectionPoolMetrics.USAGE).timer().count());
    }
}
