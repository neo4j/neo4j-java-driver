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
package org.neo4j.driver.internal.async;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.connection.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.testutil.TestUtil.setupConnectionAnswers;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.ArgumentCaptor;
import org.neo4j.bolt.connection.TelemetryApi;
import org.neo4j.bolt.connection.message.BeginMessage;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.bolt.connection.message.PullMessage;
import org.neo4j.bolt.connection.message.RunMessage;
import org.neo4j.bolt.connection.summary.BeginSummary;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.driver.AuthTokenManagers;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.BookmarkManager;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnection;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnectionProvider;
import org.neo4j.driver.internal.adaptedbolt.DriverResponseHandler;
import org.neo4j.driver.internal.adaptedbolt.summary.PullSummary;
import org.neo4j.driver.internal.security.BoltSecurityPlanManager;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.internal.util.FixedRetryLogic;
import org.neo4j.driver.testutil.TestUtil;

class LeakLoggingNetworkSessionTest {
    @Test
    void logsNothingDuringFinalizationIfClosed() throws Exception {
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var log = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(log);
        var connection = TestUtil.connectionMock();
        setupConnectionAnswers(connection, List.of(new TestUtil.MessageHandler() {
            @Override
            public List<Class<? extends Message>> messageTypes() {
                return List.of(RunMessage.class, PullMessage.class);
            }

            @Override
            public void handle(DriverResponseHandler handler) {
                handler.onRunSummary(mock(RunSummary.class));
                handler.onPullSummary(mock(PullSummary.class));
                handler.onComplete();
            }
        }));
        given(connection.close()).willReturn(completedFuture(null));
        var session = newSession(logging, connection);
        session.runAsync(new Query("query"), TransactionConfig.empty())
                .toCompletableFuture()
                .join()
                .consumeAsync()
                .toCompletableFuture()
                .join();

        finalize(session);

        verify(log, never()).error(anyString(), any(Throwable.class));
    }

    @Test
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    void logsMessageWithStacktraceDuringFinalizationIfLeaked(TestInfo testInfo) throws Exception {
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var log = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(log);
        var connection = TestUtil.connectionMock();
        setupConnectionAnswers(connection, List.of(new TestUtil.MessageHandler() {
            @Override
            public List<Class<? extends Message>> messageTypes() {
                return List.of(BeginMessage.class);
            }

            @Override
            public void handle(DriverResponseHandler handler) {
                handler.onBeginSummary(mock(BeginSummary.class));
                handler.onComplete();
            }
        }));
        var session = newSession(logging, connection);
        // begin transaction to make session obtain a connection
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        session.beginTransactionAsync(TransactionConfig.empty(), apiTelemetryWork)
                .toCompletableFuture()
                .join();

        finalize(session);

        var messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(log).error(messageCaptor.capture(), any());

        assertEquals(1, messageCaptor.getAllValues().size());

        var loggedMessage = messageCaptor.getValue();
        assertThat(loggedMessage, containsString("Neo4j Session object leaked"));
        assertThat(loggedMessage, containsString("Session was create at"));
        assertThat(
                loggedMessage,
                containsString(getClass().getSimpleName() + "."
                        + testInfo.getTestMethod().get().getName()));
    }

    private static void finalize(NetworkSession session) throws Exception {
        var finalizeMethod = session.getClass().getDeclaredMethod("finalize");
        finalizeMethod.setAccessible(true);
        finalizeMethod.invoke(session);
    }

    private static LeakLoggingNetworkSession newSession(
            @SuppressWarnings("deprecation") Logging logging, DriverBoltConnection connection) {
        return new LeakLoggingNetworkSession(
                BoltSecurityPlanManager.insecure(),
                connectionProviderMock(connection),
                new FixedRetryLogic(0),
                defaultDatabase(),
                READ,
                Collections.emptySet(),
                null,
                -1,
                logging,
                mock(BookmarkManager.class),
                NotificationConfig.defaultConfig(),
                NotificationConfig.defaultConfig(),
                null,
                true,
                AuthTokenManagers.basic(AuthTokens::none),
                mock());
    }

    private static DriverBoltConnectionProvider connectionProviderMock(DriverBoltConnection connection) {
        var provider = mock(DriverBoltConnectionProvider.class);
        when(provider.connect(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(connection));
        return provider;
    }
}
