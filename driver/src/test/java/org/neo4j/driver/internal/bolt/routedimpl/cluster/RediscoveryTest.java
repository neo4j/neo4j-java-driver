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

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.bolt.api.util.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.bolt.api.util.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.bolt.api.util.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.bolt.api.util.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.bolt.api.util.ClusterCompositionUtil.E;
import static org.neo4j.driver.testutil.TestUtil.asOrderedSet;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.neo4j.driver.exceptions.AuthTokenManagerExecutionException;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.ClusterComposition;
import org.neo4j.driver.internal.bolt.api.DefaultDomainNameResolver;
import org.neo4j.driver.internal.bolt.api.DomainNameResolver;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.ImmediateSchedulingEventExecutor;

class RediscoveryTest {
    @Test
    void shouldUseFirstRouterInTable() {
        var expectedComposition =
                new ClusterComposition(42, asOrderedSet(B, C), asOrderedSet(C, D), asOrderedSet(B), null);

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(B, expectedComposition); // first -> valid cluster composition

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);

        var rediscovery = newRediscovery(A, Collections::singleton);
        var table = routingTableMock(B);

        var actualComposition = await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1)))
                .getClusterComposition();

        assertEquals(expectedComposition, actualComposition);
        verify(table, never()).forget(B);
    }

    @Test
    void shouldSkipFailingRouters() {
        var expectedComposition =
                new ClusterComposition(42, asOrderedSet(A, B, C), asOrderedSet(B, C, D), asOrderedSet(A, B), null);

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(A, new RuntimeException("Hi!")); // first -> non-fatal failure
        responsesByAddress.put(B, new ServiceUnavailableException("Hi!")); // second -> non-fatal failure
        responsesByAddress.put(C, expectedComposition); // third -> valid cluster composition

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);

        var rediscovery = newRediscovery(A, Collections::singleton);
        var table = routingTableMock(A, B, C);

        var actualComposition = await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1)))
                .getClusterComposition();

        assertEquals(expectedComposition, actualComposition);
        verify(table).forget(A);
        verify(table).forget(B);
        verify(table, never()).forget(C);
    }

    @Test
    void shouldFailImmediatelyOnAuthError() {
        var authError = new AuthenticationException("Neo.ClientError.Security.Unauthorized", "Wrong password");

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(A, new RuntimeException("Hi!")); // first router -> non-fatal failure
        responsesByAddress.put(B, authError); // second router -> fatal auth error

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        var rediscovery = newRediscovery(A, Collections::singleton);
        var table = routingTableMock(A, B, C);

        var error = assertThrows(
                AuthenticationException.class,
                () -> await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1))));
        assertEquals(authError, error);
        verify(table).forget(A);
    }

    @Test
    void shouldUseAnotherRouterOnAuthorizationExpiredException() {
        var expectedComposition =
                new ClusterComposition(42, asOrderedSet(A, B, C), asOrderedSet(B, C, D), asOrderedSet(A, B), null);

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(
                A, new AuthorizationExpiredException("Neo.ClientError.Security.AuthorizationExpired", "message"));
        responsesByAddress.put(B, expectedComposition);

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        var rediscovery = newRediscovery(A, Collections::singleton);
        var table = routingTableMock(A, B, C);

        var actualComposition = await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1)))
                .getClusterComposition();

        assertEquals(expectedComposition, actualComposition);
        verify(table).forget(A);
        verify(table, never()).forget(B);
        verify(table, never()).forget(C);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "Neo.ClientError.Transaction.InvalidBookmark",
                "Neo.ClientError.Transaction.InvalidBookmarkMixture"
            })
    void shouldFailImmediatelyOnBookmarkErrors(String code) {
        var error = new ClientException(code, "Invalid");

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(A, new RuntimeException("Hi!"));
        responsesByAddress.put(B, error);

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        var rediscovery = newRediscovery(A, Collections::singleton);
        var table = routingTableMock(A, B, C);

        var actualError = assertThrows(
                ClientException.class,
                () -> await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1))));
        assertEquals(error, actualError);
        verify(table).forget(A);
    }

    @Test
    void shouldFailImmediatelyOnClosedPoolError() {
        var error = new IllegalStateException("Connection provider is closed.");

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(A, new RuntimeException("Hi!"));
        responsesByAddress.put(B, error);

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        var rediscovery = newRediscovery(A, Collections::singleton);
        var table = routingTableMock(A, B, C);

        var actualError = assertThrows(
                IllegalStateException.class,
                () -> await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1))));
        assertEquals(error, actualError);
        verify(table).forget(A);
    }

    @Test
    void shouldFallbackToInitialRouterWhenKnownRoutersFail() {
        var initialRouter = A;
        var expectedComposition =
                new ClusterComposition(42, asOrderedSet(C, B, A), asOrderedSet(A, B), asOrderedSet(D, E), null);

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(B, new ServiceUnavailableException("Hi!")); // first -> non-fatal failure
        responsesByAddress.put(C, new ServiceUnavailableException("Hi!")); // second -> non-fatal failure
        responsesByAddress.put(initialRouter, expectedComposition); // initial -> valid response

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        var resolver = resolverMock(initialRouter, initialRouter);
        var rediscovery = newRediscovery(initialRouter, resolver);
        var table = routingTableMock(B, C);

        var actualComposition = await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1)))
                .getClusterComposition();

        assertEquals(expectedComposition, actualComposition);
        verify(table).forget(B);
        verify(table).forget(C);
    }

    @Test
    void shouldResolveInitialRouterAddress() {
        var initialRouter = A;
        var expectedComposition =
                new ClusterComposition(42, asOrderedSet(A, B), asOrderedSet(A, B), asOrderedSet(A, B), null);

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(B, new ServiceUnavailableException("Hi!")); // first -> non-fatal failure
        responsesByAddress.put(C, new ServiceUnavailableException("Hi!")); // second -> non-fatal failure
        responsesByAddress.put(D, new IOException("Hi!")); // resolved first -> non-fatal failure
        responsesByAddress.put(E, expectedComposition); // resolved second -> valid response

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        // initial router resolved to two other addresses
        var resolver = resolverMock(initialRouter, D, E);
        var rediscovery = newRediscovery(initialRouter, resolver);
        var table = routingTableMock(B, C);

        var actualComposition = await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1)))
                .getClusterComposition();

        assertEquals(expectedComposition, actualComposition);
        verify(table).forget(B);
        verify(table).forget(C);
        verify(table).forget(D);
    }

    @Test
    void shouldResolveInitialRouterAddressUsingCustomResolver() {
        var expectedComposition =
                new ClusterComposition(42, asOrderedSet(A, B, C), asOrderedSet(A, B, C), asOrderedSet(B, E), null);

        Function<BoltServerAddress, Set<BoltServerAddress>> resolver = address -> {
            assertEquals(A, address);
            return asOrderedSet(B, C, E);
        };

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(B, new ServiceUnavailableException("Hi!")); // first -> non-fatal failure
        responsesByAddress.put(C, new ServiceUnavailableException("Hi!")); // second -> non-fatal failure
        responsesByAddress.put(E, expectedComposition); // resolved second -> valid response

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        var rediscovery = newRediscovery(A, resolver);
        var table = routingTableMock(B, C);

        var actualComposition = await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1)))
                .getClusterComposition();

        assertEquals(expectedComposition, actualComposition);
        verify(table).forget(B);
        verify(table).forget(C);
    }

    @Test
    void shouldPropagateFailureWhenResolverFails() {
        var expectedComposition =
                new ClusterComposition(42, asOrderedSet(A, B), asOrderedSet(A, B), asOrderedSet(A, B), null);

        Map<BoltServerAddress, Object> responsesByAddress = singletonMap(A, expectedComposition);
        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);

        // failing server address resolver
        @SuppressWarnings("unchecked")
        Function<BoltServerAddress, Set<BoltServerAddress>> resolver = mock(Function.class);
        when(resolver.apply(A)).thenThrow(new RuntimeException("Resolver fails!"));

        var rediscovery = newRediscovery(A, resolver);
        var table = routingTableMock();

        var error = assertThrows(
                RuntimeException.class,
                () -> await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1))));
        assertEquals("Resolver fails!", error.getMessage());

        verify(resolver).apply(A);
        verify(table, never()).forget(any());
    }

    @Test
    void shouldRecordAllErrorsWhenNoRouterRespond() {
        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        var first = new ServiceUnavailableException("Hi!");
        responsesByAddress.put(A, first); // first -> non-fatal failure
        var second = new SessionExpiredException("Hi!");
        responsesByAddress.put(B, second); // second -> non-fatal failure
        var third = new IOException("Hi!");
        responsesByAddress.put(C, third); // third -> non-fatal failure

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        var rediscovery = newRediscovery(A, Collections::singleton);
        var table = routingTableMock(A, B, C);

        var e = assertThrows(
                ServiceUnavailableException.class,
                () -> await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1))));
        assertThat(e.getMessage(), containsString("Could not perform discovery"));
        assertThat(e.getSuppressed().length, equalTo(3));
        assertThat(e.getSuppressed()[0].getCause(), equalTo(first));
        assertThat(e.getSuppressed()[1].getCause(), equalTo(second));
        assertThat(e.getSuppressed()[2].getCause(), equalTo(third));
    }

    @Test
    void shouldUseInitialRouterAfterDiscoveryReturnsNoWriters() {
        var initialRouter = A;
        var noWritersComposition = new ClusterComposition(42, asOrderedSet(D, E), emptySet(), asOrderedSet(D, E), null);
        var validComposition =
                new ClusterComposition(42, asOrderedSet(B, A), asOrderedSet(B, A), asOrderedSet(B, A), null);

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(initialRouter, validComposition); // initial -> valid composition

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        var resolver = resolverMock(initialRouter, initialRouter);
        var rediscovery = newRediscovery(initialRouter, resolver);
        RoutingTable table = new ClusterRoutingTable(defaultDatabase(), new FakeClock());
        table.update(noWritersComposition);

        var composition2 = await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1)))
                .getClusterComposition();
        assertEquals(validComposition, composition2);
    }

    @Test
    void shouldUseInitialRouterToStartWith() {
        var initialRouter = A;
        var validComposition = new ClusterComposition(42, asOrderedSet(A), asOrderedSet(A), asOrderedSet(A), null);

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(initialRouter, validComposition); // initial -> valid composition

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        var resolver = resolverMock(initialRouter, initialRouter);
        var rediscovery = newRediscovery(initialRouter, resolver);
        var table = routingTableMock(true, B, C, D);

        var composition = await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        Collections.emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1)))
                .getClusterComposition();
        assertEquals(validComposition, composition);
    }

    @Test
    void shouldUseKnownRoutersWhenInitialRouterFails() {
        var initialRouter = A;
        var validComposition =
                new ClusterComposition(42, asOrderedSet(D, E), asOrderedSet(E, D), asOrderedSet(A, B), null);

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(initialRouter, new ServiceUnavailableException("Hi")); // initial -> non-fatal error
        responsesByAddress.put(D, new IOException("Hi")); // first known -> non-fatal failure
        responsesByAddress.put(E, validComposition); // second known -> valid composition

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        var resolver = resolverMock(initialRouter, initialRouter);
        var rediscovery = newRediscovery(initialRouter, resolver);
        var table = routingTableMock(true, D, E);

        var composition = await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        Collections.emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1)))
                .getClusterComposition();
        assertEquals(validComposition, composition);
        verify(table).forget(initialRouter);
        verify(table).forget(D);
    }

    @Test
    void shouldNotLogWhenSingleRetryAttemptFails() {
        Map<BoltServerAddress, Object> responsesByAddress = singletonMap(A, new ServiceUnavailableException("Hi!"));
        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        var resolver = resolverMock(A, A);

        var eventExecutor = new ImmediateSchedulingEventExecutor();
        var logging = mock(LoggingProvider.class);
        var logger = mock(System.Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);
        Rediscovery rediscovery = new RediscoveryImpl(A, resolver, logging, DefaultDomainNameResolver.getInstance());
        var table = routingTableMock(A);

        var e = assertThrows(
                ServiceUnavailableException.class,
                () -> await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        Collections.emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1))));
        assertThat(e.getMessage(), containsString("Could not perform discovery"));

        // rediscovery should not log about retries and should not schedule any retries
        verify(logging).getLog(RediscoveryImpl.class);
        verify(logger, never())
                .log(eq(System.Logger.Level.INFO), startsWith("Unable to fetch new routing table, will try again in "));
        assertEquals(0, eventExecutor.scheduleDelays().size());
    }

    @Test
    void shouldResolveToIP() throws UnknownHostException {
        var resolver = resolverMock(A, A);
        var domainNameResolver = mock(DomainNameResolver.class);
        var localhost = InetAddress.getLocalHost();
        when(domainNameResolver.resolve(A.host())).thenReturn(new InetAddress[] {localhost});
        Rediscovery rediscovery = new RediscoveryImpl(A, resolver, NoopLoggingProvider.INSTANCE, domainNameResolver);

        var addresses = rediscovery.resolve();

        verify(resolver, times(1)).apply(A);
        verify(domainNameResolver, times(1)).resolve(A.host());
        assertEquals(1, addresses.size());
        assertEquals(new BoltServerAddress(A.host(), localhost.getHostAddress(), A.port()), addresses.get(0));
    }

    @Test
    void shouldFailImmediatelyOnAuthTokenManagerExecutionException() {
        var exception = new AuthTokenManagerExecutionException("message", mock(Throwable.class));

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(A, new RuntimeException("Hi!")); // first router -> non-fatal failure
        responsesByAddress.put(B, exception); // second router -> fatal auth error

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        var rediscovery = newRediscovery(A, Collections::singleton);
        var table = routingTableMock(A, B, C);

        var actualException = assertThrows(
                AuthTokenManagerExecutionException.class,
                () -> await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        Collections.emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1))));
        assertEquals(exception, actualException);
        verify(table).forget(A);
    }

    @Test
    void shouldFailImmediatelyOnUnsupportedFeatureException() {
        var exception = new UnsupportedFeatureException("message", mock(Throwable.class));

        Map<BoltServerAddress, Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put(A, new RuntimeException("Hi!")); // first router -> non-fatal failure
        responsesByAddress.put(B, exception); // second router -> fatal auth error

        var connectionProviderGetter = connectionProviderGetter(responsesByAddress);
        var rediscovery = newRediscovery(A, Collections::singleton);
        var table = routingTableMock(A, B, C);

        var actualException = assertThrows(
                UnsupportedFeatureException.class,
                () -> await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        Collections.emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1))));
        assertEquals(exception, actualException);
        verify(table).forget(A);
    }

    @Test
    void shouldLogScopedIPV6AddressWithStringFormattingLogger() throws UnknownHostException {
        // GIVEN
        var initialRouter = new BoltServerAddress("initialRouter", 7687);
        var connectionProviderGetter = connectionProviderGetter(Collections.emptyMap());
        var resolver = resolverMock(initialRouter, initialRouter);
        var domainNameResolver = mock(DomainNameResolver.class);
        var address = mock(InetAddress.class);
        given(address.getHostAddress()).willReturn("fe80:0:0:0:ce66:1564:db8q:94b6%6");
        given(domainNameResolver.resolve(initialRouter.host())).willReturn(new InetAddress[] {address});
        var table = routingTableMock(true);
        var pool = mock(BoltConnectionProvider.class);
        given(pool.connect(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(failedFuture(new ServiceUnavailableException("not available")));
        var logging = mock(LoggingProvider.class);
        var logger = mock(System.Logger.class);
        given(logging.getLog(any(Class.class))).willReturn(logger);
        doAnswer(invocationOnMock -> String.format(invocationOnMock.getArgument(0), invocationOnMock.getArgument(1)))
                .when(logger)
                .log(eq(System.Logger.Level.WARNING), anyString());
        var rediscovery = new RediscoveryImpl(initialRouter, resolver, logging, domainNameResolver);

        // WHEN & THEN
        assertThrows(
                ServiceUnavailableException.class,
                () -> await(rediscovery.lookupClusterComposition(
                        SecurityPlan.INSECURE,
                        table,
                        connectionProviderGetter,
                        Collections.emptySet(),
                        null,
                        null,
                        new BoltProtocolVersion(4, 1))));
    }

    private Rediscovery newRediscovery(
            BoltServerAddress initialRouter, Function<BoltServerAddress, Set<BoltServerAddress>> resolver) {
        return newRediscovery(initialRouter, resolver, NoopLoggingProvider.INSTANCE);
    }

    @SuppressWarnings("SameParameterValue")
    private Rediscovery newRediscovery(
            BoltServerAddress initialRouter,
            Function<BoltServerAddress, Set<BoltServerAddress>> resolver,
            LoggingProvider loggingProvider) {
        return new RediscoveryImpl(initialRouter, resolver, loggingProvider, DefaultDomainNameResolver.getInstance());
    }

    private Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter(
            Map<BoltServerAddress, Object> responsesByAddress) {
        var addressToProvider = new HashMap<BoltServerAddress, BoltConnectionProvider>();
        for (var entry : responsesByAddress.entrySet()) {
            var boltConnection = setupConnection(entry.getValue());

            var boltConnectionProvider = mock(BoltConnectionProvider.class);
            given(boltConnectionProvider.connect(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                    .willReturn(completedFuture(boltConnection));

            addressToProvider.put(entry.getKey(), boltConnectionProvider);
        }
        return addressToProvider::get;
    }

    private BoltConnection setupConnection(Object answer) {
        var boltConnection = mock(BoltConnection.class);
        given(boltConnection.route(any(), any(), any())).willReturn(CompletableFuture.completedStage(boltConnection));
        given(boltConnection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
            var handler = (ResponseHandler) invocationOnMock.getArguments()[0];

            if (answer instanceof ClusterComposition composition) {
                handler.onRouteSummary(() -> composition);
            } else if (answer instanceof Throwable throwable) {
                handler.onError(throwable);
            }
            handler.onComplete();

            return CompletableFuture.completedStage(null);
        });
        given(boltConnection.close()).willReturn(CompletableFuture.completedStage(null));
        return boltConnection;
    }

    private static Function<BoltServerAddress, Set<BoltServerAddress>> resolverMock(
            BoltServerAddress address, BoltServerAddress... resolved) {
        @SuppressWarnings("unchecked")
        Function<BoltServerAddress, Set<BoltServerAddress>> resolverMock = Mockito.mock(Function.class);
        given(resolverMock.apply(address)).willReturn(asOrderedSet(resolved));
        return resolverMock;
    }

    private static RoutingTable routingTableMock(BoltServerAddress... routers) {
        return routingTableMock(false, routers);
    }

    private static RoutingTable routingTableMock(boolean preferInitialRouter, BoltServerAddress... routers) {
        var routingTable = Mockito.mock(RoutingTable.class);
        when(routingTable.routers()).thenReturn(Arrays.asList(routers));
        when(routingTable.database()).thenReturn(defaultDatabase());
        when(routingTable.preferInitialRouter()).thenReturn(preferInitialRouter);
        return routingTable;
    }
}
