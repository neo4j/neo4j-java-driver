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
package org.neo4j.driver.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.driver.BookmarkManager;
import org.neo4j.driver.BookmarkManagerConfig;
import org.neo4j.driver.BookmarkManagers;
import org.neo4j.driver.Config;
import org.neo4j.driver.Metrics;
import org.neo4j.driver.Query;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.Result;
import org.neo4j.driver.ResultTransformer;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.metrics.DevNullMetricsProvider;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.util.Clock;

class InternalDriverTest {
    @Test
    @SuppressWarnings("unchecked")
    void shouldExecuteQuery() {
        var sessionFactory = sessionFactoryMock();
        var driver = spy(newDriver(sessionFactory));
        var query = new Query("RETURN 1");
        ResultTransformer<Object> resultTransformer = mock(ResultTransformer.class);
        var bookmarkManager = mock(BookmarkManager.class);
        var config = QueryConfig.builder(resultTransformer)
                .withDatabase("database")
                .withImpersonatedUser("user")
                .withBookmarkManager(bookmarkManager)
                .build();
        var session = mock(Session.class);
        given(driver.session(any(SessionConfig.class))).willReturn(session);
        var result = mock(Result.class);
        var txContext = mock(TransactionContext.class);
        var expectedResult = new Object();
        given(txContext.run(any(Query.class))).willReturn(result);
        given(resultTransformer.transform(result)).willReturn(expectedResult);
        given(session.executeWrite(any(TransactionCallback.class))).willAnswer(invocation -> {
            TransactionCallback<Object> callback = invocation.getArgument(0);
            return callback.execute(txContext);
        });

        var actualResult = driver.executeQuery(query, config);

        var argument = ArgumentCaptor.forClass(SessionConfig.class);
        then(driver).should(times(2)).session(argument.capture());
        var sessionConfig = argument.getValue();
        assertEquals(config.database(), sessionConfig.database());
        assertEquals(config.bookmarkManager(mock(BookmarkManager.class)), sessionConfig.bookmarkManager());
        then(session).should().executeWrite(any(TransactionCallback.class));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldNotAllowReturningResultInExecuteQuery() {
        var sessionFactory = sessionFactoryMock();
        var driver = spy(newDriver(sessionFactory));
        var query = new Query("RETURN 1");
        ResultTransformer<Object> resultTransformer = mock(ResultTransformer.class);
        var bookmarkManager = mock(BookmarkManager.class);
        var config = QueryConfig.builder(resultTransformer)
                .withDatabase("database")
                .withImpersonatedUser("user")
                .withBookmarkManager(bookmarkManager)
                .build();
        var session = mock(Session.class);
        given(driver.session(any(SessionConfig.class))).willReturn(session);
        var result = mock(Result.class);
        var txContext = mock(TransactionContext.class);
        given(txContext.run(any(Query.class))).willReturn(result);
        given(resultTransformer.transform(result)).willReturn(result);
        given(session.executeWrite(any(TransactionCallback.class))).willAnswer(invocation -> {
            TransactionCallback<Object> callback = invocation.getArgument(0);
            return callback.execute(txContext);
        });

        var exception = assertThrows(IllegalStateException.class, () -> driver.executeQuery(query, config));

        assertTrue(exception.getMessage().contains("Illegal result returned"));
    }

    @Test
    void shouldCloseSessionFactory() {
        SessionFactory sessionFactory = sessionFactoryMock();
        InternalDriver driver = newDriver(sessionFactory);

        assertNull(await(driver.closeAsync()));
        verify(sessionFactory).close();
    }

    @Test
    void shouldNotCloseSessionFactoryMultipleTimes() {
        SessionFactory sessionFactory = sessionFactoryMock();
        InternalDriver driver = newDriver(sessionFactory);

        assertNull(await(driver.closeAsync()));
        assertNull(await(driver.closeAsync()));
        assertNull(await(driver.closeAsync()));

        verify(sessionFactory).close();
    }

    @Test
    void shouldVerifyConnectivity() {
        SessionFactory sessionFactory = sessionFactoryMock();
        CompletableFuture<Void> connectivityStage = completedWithNull();
        when(sessionFactory.verifyConnectivity()).thenReturn(connectivityStage);

        InternalDriver driver = newDriver(sessionFactory);

        assertEquals(connectivityStage, driver.verifyConnectivityAsync());
    }

    @Test
    void shouldThrowWhenUnableToVerifyConnectivity() {
        SessionFactory sessionFactory = mock(SessionFactory.class);
        ServiceUnavailableException error = new ServiceUnavailableException("Hello");
        when(sessionFactory.verifyConnectivity()).thenReturn(failedFuture(error));
        InternalDriver driver = newDriver(sessionFactory);

        ServiceUnavailableException e =
                assertThrows(ServiceUnavailableException.class, () -> await(driver.verifyConnectivityAsync()));
        assertEquals(e.getMessage(), "Hello");
    }

    @Test
    void shouldThrowClientExceptionIfMetricsNotEnabled() throws Throwable {
        // Given
        InternalDriver driver = newDriver(false);

        // When
        ClientException error = assertThrows(ClientException.class, driver::metrics);

        // Then
        assertTrue(error.getMessage().contains("Driver metrics are not enabled."));
    }

    @Test
    void shouldReturnMetricsIfMetricsEnabled() {
        // Given
        InternalDriver driver = newDriver(true);

        // When
        Metrics metrics = driver.metrics();

        // Then we shall have no problem to get the metrics
        assertNotNull(metrics);
    }

    private static InternalDriver newDriver(SessionFactory sessionFactory) {
        return new InternalDriver(
                BookmarkManagers.defaultManager(BookmarkManagerConfig.builder().build()),
                SecurityPlanImpl.insecure(),
                sessionFactory,
                DevNullMetricsProvider.INSTANCE,
                DEV_NULL_LOGGING);
    }

    private static SessionFactory sessionFactoryMock() {
        SessionFactory sessionFactory = mock(SessionFactory.class);
        when(sessionFactory.close()).thenReturn(completedWithNull());
        return sessionFactory;
    }

    private static InternalDriver newDriver(boolean isMetricsEnabled) {
        SessionFactory sessionFactory = sessionFactoryMock();
        Config config = Config.defaultConfig();
        if (isMetricsEnabled) {
            config = Config.builder().withDriverMetrics().build();
        }

        MetricsProvider metricsProvider = DriverFactory.getOrCreateMetricsProvider(config, Clock.SYSTEM);
        return new InternalDriver(
                BookmarkManagers.defaultManager(BookmarkManagerConfig.builder().build()),
                SecurityPlanImpl.insecure(),
                sessionFactory,
                metricsProvider,
                DEV_NULL_LOGGING);
    }
}
