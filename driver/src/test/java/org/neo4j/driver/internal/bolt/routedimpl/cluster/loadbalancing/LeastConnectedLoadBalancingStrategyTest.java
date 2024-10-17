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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.neo4j.driver.internal.bolt.api.util.ClusterCompositionUtil.A;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;

class LeastConnectedLoadBalancingStrategyTest {
    @Mock
    private Function<BoltServerAddress, Integer> inUseFunction;

    private LeastConnectedLoadBalancingStrategy strategy;

    @BeforeEach
    @SuppressWarnings("resource")
    void setUp() {
        openMocks(this);
        strategy = new LeastConnectedLoadBalancingStrategy(inUseFunction, NoopLoggingProvider.INSTANCE);
        given(inUseFunction.apply(any())).willReturn(0);
    }

    @Test
    void shouldHandleEmptyReaders() {
        assertNull(strategy.selectReader(Collections.emptyList()));
    }

    @Test
    void shouldHandleEmptyWriters() {
        assertNull(strategy.selectWriter(Collections.emptyList()));
    }

    @Test
    void shouldHandleSingleReaderWithoutActiveConnections() {
        var address = new BoltServerAddress("reader", 9999);

        assertEquals(address, strategy.selectReader(Collections.singletonList(address)));
    }

    @Test
    void shouldHandleSingleWriterWithoutActiveConnections() {
        var address = new BoltServerAddress("writer", 9999);

        assertEquals(address, strategy.selectWriter(Collections.singletonList(address)));
    }

    @Test
    void shouldHandleSingleReaderWithActiveConnections() {
        var address = new BoltServerAddress("reader", 9999);
        when(inUseFunction.apply(address)).thenReturn(42);

        assertEquals(address, strategy.selectReader(Collections.singletonList(address)));
    }

    @Test
    void shouldHandleSingleWriterWithActiveConnections() {
        var address = new BoltServerAddress("writer", 9999);
        when(inUseFunction.apply(address)).thenReturn(24);

        assertEquals(address, strategy.selectWriter(Collections.singletonList(address)));
    }

    @Test
    void shouldHandleMultipleReadersWithActiveConnections() {
        var address1 = new BoltServerAddress("reader", 1);
        var address2 = new BoltServerAddress("reader", 2);
        var address3 = new BoltServerAddress("reader", 3);

        when(inUseFunction.apply(address1)).thenReturn(3);
        when(inUseFunction.apply(address2)).thenReturn(4);
        when(inUseFunction.apply(address3)).thenReturn(1);

        assertEquals(address3, strategy.selectReader(Arrays.asList(address1, address2, address3)));
    }

    @Test
    void shouldHandleMultipleWritersWithActiveConnections() {
        var address1 = new BoltServerAddress("writer", 1);
        var address2 = new BoltServerAddress("writer", 2);
        var address3 = new BoltServerAddress("writer", 3);
        var address4 = new BoltServerAddress("writer", 4);

        when(inUseFunction.apply(address1)).thenReturn(5);
        when(inUseFunction.apply(address2)).thenReturn(6);
        when(inUseFunction.apply(address3)).thenReturn(0);
        when(inUseFunction.apply(address4)).thenReturn(1);

        assertEquals(address3, strategy.selectWriter(Arrays.asList(address1, address2, address3, address4)));
    }

    @Test
    void shouldReturnDifferentReaderOnEveryInvocationWhenNoActiveConnections() {
        var address1 = new BoltServerAddress("reader", 1);
        var address2 = new BoltServerAddress("reader", 2);
        var address3 = new BoltServerAddress("reader", 3);

        assertEquals(address1, strategy.selectReader(Arrays.asList(address1, address2, address3)));
        assertEquals(address2, strategy.selectReader(Arrays.asList(address1, address2, address3)));
        assertEquals(address3, strategy.selectReader(Arrays.asList(address1, address2, address3)));

        assertEquals(address1, strategy.selectReader(Arrays.asList(address1, address2, address3)));
        assertEquals(address2, strategy.selectReader(Arrays.asList(address1, address2, address3)));
        assertEquals(address3, strategy.selectReader(Arrays.asList(address1, address2, address3)));
    }

    @Test
    void shouldReturnDifferentWriterOnEveryInvocationWhenNoActiveConnections() {
        var address1 = new BoltServerAddress("writer", 1);
        var address2 = new BoltServerAddress("writer", 2);

        assertEquals(address1, strategy.selectReader(Arrays.asList(address1, address2)));
        assertEquals(address2, strategy.selectReader(Arrays.asList(address1, address2)));

        assertEquals(address1, strategy.selectReader(Arrays.asList(address1, address2)));
        assertEquals(address2, strategy.selectReader(Arrays.asList(address1, address2)));
    }

    @Test
    void shouldTraceLogWhenNoAddressSelected() {
        var logging = mock(LoggingProvider.class);
        var logger = mock(System.Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);

        LoadBalancingStrategy strategy = new LeastConnectedLoadBalancingStrategy(inUseFunction, logging);

        strategy.selectReader(Collections.emptyList());
        strategy.selectWriter(Collections.emptyList());

        verify(logger).log(eq(System.Logger.Level.TRACE), startsWith("Unable to select"), eq("reader"));
        verify(logger).log(eq(System.Logger.Level.TRACE), startsWith("Unable to select"), eq("writer"));
    }

    @Test
    void shouldTraceLogSelectedAddress() {
        var logging = mock(LoggingProvider.class);
        var logger = mock(System.Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);

        when(inUseFunction.apply(any(BoltServerAddress.class))).thenReturn(42);

        LoadBalancingStrategy strategy = new LeastConnectedLoadBalancingStrategy(inUseFunction, logging);

        strategy.selectReader(Collections.singletonList(A));
        strategy.selectWriter(Collections.singletonList(A));

        verify(logger).log(eq(System.Logger.Level.TRACE), startsWith("Selected"), eq("reader"), eq(A), eq(42));
        verify(logger).log(eq(System.Logger.Level.TRACE), startsWith("Selected"), eq("writer"), eq(A), eq(42));
    }
}
