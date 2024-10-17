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
package org.neo4j.driver.internal.bolt.pooledimpl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.MockitoAnnotations.openMocks;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.AuthData;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltAgentUtil;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltConnectionState;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.DatabaseNameUtil;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.MetricsListener;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.api.exception.MinVersionAcquisitionException;
import org.neo4j.driver.internal.bolt.api.summary.ResetSummary;

class PooledBoltConnectionProviderTest {
    PooledBoltConnectionProvider provider;

    @Mock
    BoltConnectionProvider upstreamProvider;

    @Mock
    LoggingProvider loggingProvider;

    @Mock
    Clock clock;

    @Mock
    MetricsListener metricsListener;

    @Mock
    Consumer<DatabaseName> databaseNameConsumer;

    @Mock
    BoltConnection connection;

    @Mock
    Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier;

    final int maxSize = 2;
    final long acquisitionTimeout = 5000;
    final long maxLifetime = 60000;
    final long idleBeforeTest = 30000;
    final BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
    final RoutingContext context = RoutingContext.EMPTY;
    final BoltAgent boltAgent = BoltAgentUtil.VALUE;
    final String userAgent = "agent";
    final int timeout = 1000;

    final SecurityPlan securityPlan = SecurityPlan.INSECURE;
    final DatabaseName databaseName = DatabaseNameUtil.defaultDatabase();
    final AccessMode mode = AccessMode.WRITE;
    final Set<String> bookmarks = Set.of("bookmark1", "bookmark2");
    final String impersonatedUser = null;
    final BoltProtocolVersion minVersion = new BoltProtocolVersion(5, 6);
    final NotificationConfig notificationConfig = NotificationConfig.defaultConfig();

    @BeforeEach
    @SuppressWarnings("resource")
    void beforeEach() {
        openMocks(this);
        given(loggingProvider.getLog(any(Class.class))).willReturn(mock(System.Logger.class));
        given(authMapStageSupplier.get()).willReturn(CompletableFuture.completedStage(Map.of()));
        provider = new PooledBoltConnectionProvider(
                upstreamProvider, maxSize, acquisitionTimeout, maxLifetime, idleBeforeTest, clock, loggingProvider);
        provider.init(address, context, boltAgent, userAgent, timeout, metricsListener);
    }

    @Test
    void shouldInitUpstream() {
        // then
        then(upstreamProvider).should().init(address, context, boltAgent, userAgent, timeout, metricsListener);
    }

    @Test
    void shouldCreateNewConnection() {
        // given
        given(upstreamProvider.connect(
                        eq(securityPlan),
                        eq(databaseName),
                        any(),
                        eq(mode),
                        eq(bookmarks),
                        eq(null),
                        eq(minVersion),
                        eq(notificationConfig),
                        any()))
                .willReturn(CompletableFuture.completedStage(connection));

        // when
        var connection = provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer)
                .toCompletableFuture()
                .join();

        // then
        var pooledConnection = assertInstanceOf(PooledBoltConnection.class, connection);
        assertEquals(this.connection, pooledConnection.delegate());
        then(upstreamProvider)
                .should()
                .connect(
                        eq(securityPlan),
                        eq(databaseName),
                        any(),
                        eq(mode),
                        eq(bookmarks),
                        eq(null),
                        eq(minVersion),
                        eq(notificationConfig),
                        any());
        assertEquals(1, provider.inUse());
        assertEquals(1, provider.size());
    }

    @Test
    void shouldTimeout() {
        // given
        var acquisitionTimeout = TimeUnit.SECONDS.toMillis(1);
        given(upstreamProvider.connect(
                        eq(securityPlan),
                        eq(databaseName),
                        any(),
                        eq(mode),
                        eq(bookmarks),
                        eq(null),
                        eq(minVersion),
                        eq(notificationConfig),
                        any()))
                .willReturn(CompletableFuture.completedStage(connection));
        provider = new PooledBoltConnectionProvider(
                upstreamProvider, 1, acquisitionTimeout, maxLifetime, idleBeforeTest, clock, loggingProvider);
        provider.init(address, context, boltAgent, userAgent, timeout, metricsListener);
        provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer)
                .toCompletableFuture()
                .join();

        // when
        var connectionStage = provider.connect(
                securityPlan,
                databaseName,
                authMapStageSupplier,
                mode,
                bookmarks,
                null,
                minVersion,
                notificationConfig,
                databaseNameConsumer);

        // then
        var completionException = assertThrows(
                CompletionException.class,
                () -> connectionStage.toCompletableFuture().join());
        assertInstanceOf(TimeoutException.class, completionException.getCause());
    }

    @Test
    void shouldReturnConnectionToPool() {
        // given
        given(connection.reset()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
            var handler = (ResponseHandler) invocationOnMock.getArgument(0);
            handler.onResetSummary(mock(ResetSummary.class));
            handler.onComplete();
            return CompletableFuture.completedStage(null);
        });
        given(upstreamProvider.connect(
                        eq(securityPlan),
                        eq(databaseName),
                        any(),
                        eq(mode),
                        eq(bookmarks),
                        eq(null),
                        eq(minVersion),
                        eq(notificationConfig),
                        any()))
                .willReturn(CompletableFuture.completedStage(connection));
        var connection = provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer)
                .toCompletableFuture()
                .join();

        // when
        connection.close().toCompletableFuture().join();

        // then
        assertEquals(0, provider.inUse());
        assertEquals(1, provider.size());
    }

    @Test
    void shouldUseExistingConnection() {
        // given
        given(connection.reset()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
            var handler = (ResponseHandler) invocationOnMock.getArgument(0);
            handler.onResetSummary(mock(ResetSummary.class));
            handler.onComplete();
            return CompletableFuture.completedStage(null);
        });
        given(connection.state()).willReturn(BoltConnectionState.OPEN);
        given(connection.protocolVersion()).willReturn(minVersion);
        var authData = mock(AuthData.class);
        given(authData.authAckMillis()).willReturn(0L);
        given(authData.authMap()).willReturn(Collections.emptyMap());
        given(connection.authData()).willReturn(CompletableFuture.completedStage(authData));
        given(upstreamProvider.connect(
                        eq(securityPlan),
                        eq(databaseName),
                        any(),
                        eq(mode),
                        eq(bookmarks),
                        eq(null),
                        eq(minVersion),
                        eq(notificationConfig),
                        any()))
                .willReturn(CompletableFuture.completedStage(connection));
        provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer)
                .toCompletableFuture()
                .join()
                .close()
                .toCompletableFuture()
                .join();
        BDDMockito.reset(upstreamProvider);

        // when
        var connection = provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer)
                .toCompletableFuture()
                .join();

        // then
        var pooledConnection = assertInstanceOf(PooledBoltConnection.class, connection);
        assertEquals(this.connection, pooledConnection.delegate());
        then(upstreamProvider).shouldHaveNoInteractions();
        assertEquals(1, provider.inUse());
        assertEquals(1, provider.size());
    }

    @Test
    void shouldClose() {
        // given
        given(connection.reset()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
            var handler = (ResponseHandler) invocationOnMock.getArgument(0);
            handler.onResetSummary(mock(ResetSummary.class));
            handler.onComplete();
            return CompletableFuture.completedStage(null);
        });
        given(connection.state()).willReturn(BoltConnectionState.OPEN);
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        given(upstreamProvider.close()).willReturn(CompletableFuture.completedStage(null));
        given(upstreamProvider.connect(
                        eq(securityPlan),
                        eq(databaseName),
                        any(),
                        eq(mode),
                        eq(bookmarks),
                        eq(null),
                        eq(minVersion),
                        eq(notificationConfig),
                        any()))
                .willReturn(CompletableFuture.completedStage(connection));
        provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer)
                .toCompletableFuture()
                .join()
                .close()
                .toCompletableFuture()
                .join();

        // when
        provider.close().toCompletableFuture().join();

        // then
        then(connection).should().close();
        then(upstreamProvider).should().close();
    }

    @Test
    void shouldVerifyConnectivity() {
        // given
        given(connection.reset()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
            var handler = (ResponseHandler) invocationOnMock.getArgument(0);
            handler.onResetSummary(mock(ResetSummary.class));
            handler.onComplete();
            return CompletableFuture.completedStage(null);
        });
        given(upstreamProvider.connect(
                        eq(securityPlan),
                        eq(null),
                        any(),
                        eq(AccessMode.WRITE),
                        eq(Collections.emptySet()),
                        eq(null),
                        eq(null),
                        eq(null),
                        any()))
                .willReturn(CompletableFuture.completedStage(connection));

        // when
        provider.verifyConnectivity(SecurityPlan.INSECURE, Collections.emptyMap())
                .toCompletableFuture()
                .join();

        // then
        then(upstreamProvider)
                .should()
                .connect(
                        eq(securityPlan),
                        eq(null),
                        any(),
                        eq(AccessMode.WRITE),
                        eq(Collections.emptySet()),
                        eq(null),
                        eq(null),
                        eq(null),
                        any());
        then(connection).should().reset();
        then(connection).should().flush(any());
    }

    @ParameterizedTest
    @MethodSource("supportsMultiDbParams")
    void shouldSupportMultiDb(BoltProtocolVersion boltProtocolVersion, boolean expectedToSupport) {
        // given
        given(connection.protocolVersion()).willReturn(boltProtocolVersion);
        given(connection.reset()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
            var handler = (ResponseHandler) invocationOnMock.getArgument(0);
            handler.onResetSummary(mock(ResetSummary.class));
            handler.onComplete();
            return CompletableFuture.completedStage(null);
        });
        given(upstreamProvider.connect(
                        eq(securityPlan),
                        eq(null),
                        any(),
                        eq(AccessMode.WRITE),
                        eq(Collections.emptySet()),
                        eq(null),
                        eq(null),
                        eq(null),
                        any()))
                .willReturn(CompletableFuture.completedStage(connection));

        // when
        var supports = provider.supportsMultiDb(SecurityPlan.INSECURE, Collections.emptyMap())
                .toCompletableFuture()
                .join();

        // then
        assertEquals(expectedToSupport, supports);
        then(upstreamProvider)
                .should()
                .connect(
                        eq(securityPlan),
                        eq(null),
                        any(),
                        eq(AccessMode.WRITE),
                        eq(Collections.emptySet()),
                        eq(null),
                        eq(null),
                        eq(null),
                        any());
        then(connection).should().reset();
        then(connection).should().flush(any());
    }

    private static Stream<Arguments> supportsMultiDbParams() {
        return Stream.of(
                Arguments.arguments(new BoltProtocolVersion(4, 0), true),
                Arguments.arguments(new BoltProtocolVersion(3, 5), false));
    }

    @ParameterizedTest
    @MethodSource("supportsSessionAuthParams")
    void shouldSupportsSessionAuth(BoltProtocolVersion boltProtocolVersion, boolean expectedToSupport) {
        // given
        given(connection.protocolVersion()).willReturn(boltProtocolVersion);
        given(connection.reset()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
            var handler = (ResponseHandler) invocationOnMock.getArgument(0);
            handler.onResetSummary(mock(ResetSummary.class));
            handler.onComplete();
            return CompletableFuture.completedStage(null);
        });
        given(upstreamProvider.connect(
                        eq(securityPlan),
                        eq(null),
                        any(),
                        eq(AccessMode.WRITE),
                        eq(Collections.emptySet()),
                        eq(null),
                        eq(null),
                        eq(null),
                        any()))
                .willReturn(CompletableFuture.completedStage(connection));

        // when
        var supports = provider.supportsSessionAuth(SecurityPlan.INSECURE, Collections.emptyMap())
                .toCompletableFuture()
                .join();

        // then
        assertEquals(expectedToSupport, supports);
        then(upstreamProvider)
                .should()
                .connect(
                        eq(securityPlan),
                        eq(null),
                        any(),
                        eq(AccessMode.WRITE),
                        eq(Collections.emptySet()),
                        eq(null),
                        eq(null),
                        eq(null),
                        any());
        then(connection).should().reset();
        then(connection).should().flush(any());
    }

    private static Stream<Arguments> supportsSessionAuthParams() {
        return Stream.of(
                Arguments.arguments(new BoltProtocolVersion(5, 1), true),
                Arguments.arguments(new BoltProtocolVersion(5, 0), false));
    }

    @Test
    void shouldThrowOnLowerVersion() {
        // given
        given(connection.protocolVersion()).willReturn(new BoltProtocolVersion(5, 0));
        given(connection.state()).willReturn(BoltConnectionState.OPEN);
        given(connection.reset()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
            var handler = (ResponseHandler) invocationOnMock.getArgument(0);
            handler.onResetSummary(mock(ResetSummary.class));
            handler.onComplete();
            return CompletableFuture.completedStage(null);
        });
        given(upstreamProvider.connect(
                        eq(securityPlan),
                        eq(databaseName),
                        any(),
                        eq(mode),
                        eq(bookmarks),
                        eq(null),
                        eq(minVersion),
                        eq(notificationConfig),
                        any()))
                .willReturn(CompletableFuture.completedStage(connection));
        provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer)
                .toCompletableFuture()
                .join()
                .close()
                .toCompletableFuture()
                .join();

        // when
        var future = provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        new BoltProtocolVersion(5, 5),
                        NotificationConfig.defaultConfig(),
                        databaseNameConsumer)
                .toCompletableFuture();

        // then
        var exception = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(MinVersionAcquisitionException.class, exception.getCause());
    }

    @Test
    void shouldTestMaxLifetime() {
        // given
        given(connection.protocolVersion()).willReturn(minVersion);
        given(connection.state()).willReturn(BoltConnectionState.OPEN);
        given(connection.reset()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
            var handler = (ResponseHandler) invocationOnMock.getArgument(0);
            handler.onResetSummary(mock(ResetSummary.class));
            handler.onComplete();
            return CompletableFuture.completedStage(null);
        });
        var connection2 = mock(BoltConnection.class);
        given(upstreamProvider.connect(
                        eq(securityPlan),
                        eq(databaseName),
                        any(),
                        eq(mode),
                        eq(bookmarks),
                        eq(null),
                        eq(minVersion),
                        eq(notificationConfig),
                        any()))
                .willReturn(CompletableFuture.completedStage(connection))
                .willReturn(CompletableFuture.completedStage(connection2));
        provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer)
                .toCompletableFuture()
                .join()
                .close()
                .toCompletableFuture()
                .join();
        given(clock.millis()).willReturn(maxLifetime + 1);

        // when
        var anotherConnection = provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        minVersion,
                        NotificationConfig.defaultConfig(),
                        databaseNameConsumer)
                .toCompletableFuture()
                .join();

        // then
        assertEquals(1, provider.inUse());
        assertEquals(1, provider.size());
        assertEquals(connection2, ((PooledBoltConnection) anotherConnection).delegate());
        then(connection).should().close();
    }

    @Test
    void shouldTestLiveness() {
        // given
        given(connection.protocolVersion()).willReturn(minVersion);
        given(connection.state()).willReturn(BoltConnectionState.OPEN);
        given(connection.reset()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
            var handler = (ResponseHandler) invocationOnMock.getArgument(0);
            handler.onResetSummary(mock(ResetSummary.class));
            handler.onComplete();
            return CompletableFuture.completedStage(null);
        });
        var authData = mock(AuthData.class);
        given(authData.authAckMillis()).willReturn(0L);
        given(authData.authMap()).willReturn(Collections.emptyMap());
        given(connection.authData()).willReturn(CompletableFuture.completedStage(authData));
        given(upstreamProvider.connect(
                        eq(securityPlan),
                        eq(databaseName),
                        any(),
                        eq(mode),
                        eq(bookmarks),
                        eq(null),
                        eq(minVersion),
                        eq(notificationConfig),
                        any()))
                .willReturn(CompletableFuture.completedStage(connection));
        provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer)
                .toCompletableFuture()
                .join()
                .close()
                .toCompletableFuture()
                .join();
        given(clock.millis()).willReturn(idleBeforeTest + 1);

        // when
        var actualConnection = provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        minVersion,
                        NotificationConfig.defaultConfig(),
                        databaseNameConsumer)
                .toCompletableFuture()
                .join();

        // then
        assertEquals(connection, ((PooledBoltConnection) actualConnection).delegate());
        then(connection).should(times(2)).reset();
        then(connection).should(times(2)).flush(any());
    }

    @Test
    void shouldPipelineReauth() {
        // given
        given(connection.protocolVersion()).willReturn(minVersion);
        given(connection.state()).willReturn(BoltConnectionState.OPEN);
        given(connection.reset()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
            var handler = (ResponseHandler) invocationOnMock.getArgument(0);
            handler.onResetSummary(mock(ResetSummary.class));
            handler.onComplete();
            return CompletableFuture.completedStage(null);
        });
        given(connection.logoff()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.logon(Map.of("key", Values.value("value"))))
                .willReturn(CompletableFuture.completedStage(connection));
        var authData = mock(AuthData.class);
        given(authData.authAckMillis()).willReturn(0L);
        given(authData.authMap()).willReturn(Collections.emptyMap());
        given(connection.authData()).willReturn(CompletableFuture.completedStage(authData));
        given(authMapStageSupplier.get())
                .willReturn(CompletableFuture.completedStage(Collections.emptyMap()))
                .willReturn(CompletableFuture.completedStage(Map.of("key", Values.value("value"))));
        given(upstreamProvider.connect(
                        eq(securityPlan),
                        eq(databaseName),
                        any(),
                        eq(mode),
                        eq(bookmarks),
                        eq(null),
                        eq(minVersion),
                        eq(notificationConfig),
                        any()))
                .willReturn(CompletableFuture.completedStage(connection));
        provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer)
                .toCompletableFuture()
                .join()
                .close()
                .toCompletableFuture()
                .join();

        // when
        var actualConnection = provider.connect(
                        securityPlan,
                        databaseName,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        null,
                        minVersion,
                        NotificationConfig.defaultConfig(),
                        databaseNameConsumer)
                .toCompletableFuture()
                .join();

        // then
        assertEquals(connection, ((PooledBoltConnection) actualConnection).delegate());
        then(connection).should().logoff();
        then(connection).should().logon(Map.of("key", Values.value("value")));
    }
}
