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
package org.neo4j.driver.internal.async;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.testutil.TestUtil.DEFAULT_TEST_PROTOCOL;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.ArgumentCaptor;
import org.neo4j.driver.BookmarkManager;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.FixedRetryLogic;
import org.neo4j.driver.testutil.TestUtil;

class LeakLoggingNetworkSessionTest {
    @Test
    void logsNothingDuringFinalizationIfClosed() throws Exception {
        var logging = mock(Logging.class);
        var log = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(log);
        var session = newSession(logging, false);

        finalize(session);

        verify(log, never()).error(anyString(), any(Throwable.class));
    }

    @Test
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    void logsMessageWithStacktraceDuringFinalizationIfLeaked(TestInfo testInfo) throws Exception {
        var logging = mock(Logging.class);
        var log = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(log);
        var session = newSession(logging, true);
        // begin transaction to make session obtain a connection
        session.beginTransactionAsync(TransactionConfig.empty());

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

    private static LeakLoggingNetworkSession newSession(Logging logging, boolean openConnection) {
        return new LeakLoggingNetworkSession(
                connectionProviderMock(openConnection),
                new FixedRetryLogic(0),
                defaultDatabase(),
                READ,
                Collections.emptySet(),
                null,
                FetchSizeUtil.UNLIMITED_FETCH_SIZE,
                logging,
                mock(BookmarkManager.class),
                null,
                null);
    }

    private static ConnectionProvider connectionProviderMock(boolean openConnection) {
        var provider = mock(ConnectionProvider.class);
        var connection = connectionMock(openConnection);
        when(provider.acquireConnection(any(ConnectionContext.class))).thenReturn(completedFuture(connection));
        return provider;
    }

    private static Connection connectionMock(boolean open) {
        var connection = TestUtil.connectionMock(DEFAULT_TEST_PROTOCOL);
        when(connection.isOpen()).thenReturn(open);
        return connection;
    }
}
