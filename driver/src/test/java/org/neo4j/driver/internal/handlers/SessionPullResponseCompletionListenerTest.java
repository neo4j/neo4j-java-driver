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
package org.neo4j.driver.internal.handlers;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.value;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Query;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ConnectionReadTimeoutException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.messaging.v43.BoltProtocolV43;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;

class SessionPullResponseCompletionListenerTest {
    @Test
    void shouldReleaseConnectionOnSuccess() {
        var connection = newConnectionMock();
        PullResponseCompletionListener listener =
                new SessionPullResponseCompletionListener(connection, (ignored) -> {});
        var handler = newHandler(connection, listener);

        handler.onSuccess(emptyMap());

        verify(connection).release();
    }

    @Test
    void shouldReleaseConnectionOnFailure() {
        var connection = newConnectionMock();
        PullResponseCompletionListener listener =
                new SessionPullResponseCompletionListener(connection, (ignored) -> {});
        var handler = newHandler(connection, listener);

        handler.onFailure(new RuntimeException());

        verify(connection).release();
    }

    @Test
    void shouldUpdateBookmarksOnSuccess() {
        var connection = newConnectionMock();
        var bookmarkValue = "neo4j:bookmark:v1:tx42";
        @SuppressWarnings("unchecked")
        Consumer<DatabaseBookmark> bookmarkConsumer = mock(Consumer.class);
        PullResponseCompletionListener listener =
                new SessionPullResponseCompletionListener(connection, bookmarkConsumer);
        var handler = newHandler(connection, listener);

        handler.onSuccess(singletonMap("bookmark", value(bookmarkValue)));

        verify(bookmarkConsumer).accept(new DatabaseBookmark(null, InternalBookmark.parse(bookmarkValue)));
    }

    @Test
    void shouldReleaseConnectionImmediatelyOnAuthorizationExpiredExceptionFailure() {
        var connection = newConnectionMock();
        PullResponseCompletionListener listener =
                new SessionPullResponseCompletionListener(connection, (ignored) -> {});
        var handler = newHandler(connection, listener);
        var exception = new AuthorizationExpiredException("code", "message");

        handler.onFailure(exception);

        verify(connection).terminateAndRelease(AuthorizationExpiredException.DESCRIPTION);
        verify(connection, never()).release();
    }

    @Test
    void shouldReleaseConnectionImmediatelyOnConnectionReadTimeoutExceptionFailure() {
        var connection = newConnectionMock();
        PullResponseCompletionListener listener =
                new SessionPullResponseCompletionListener(connection, (ignored) -> {});
        var handler = newHandler(connection, listener);

        handler.onFailure(ConnectionReadTimeoutException.INSTANCE);

        verify(connection).terminateAndRelease(ConnectionReadTimeoutException.INSTANCE.getMessage());
        verify(connection, never()).release();
    }

    private static ResponseHandler newHandler(Connection connection, PullResponseCompletionListener listener) {
        var runHandler = new RunResponseHandler(
                new CompletableFuture<>(), BoltProtocolV3.METADATA_EXTRACTOR, mock(Connection.class), null);
        var handler = new BasicPullResponseHandler(
                new Query("RETURN 1"), runHandler, connection, BoltProtocolV3.METADATA_EXTRACTOR, listener);
        handler.installRecordConsumer((record, throwable) -> {});
        handler.installSummaryConsumer((resultSummary, throwable) -> {});
        return handler;
    }

    private static Connection newConnectionMock() {
        var connection = mock(Connection.class);
        when(connection.serverAddress()).thenReturn(BoltServerAddress.LOCAL_DEFAULT);
        when(connection.protocol()).thenReturn(BoltProtocolV43.INSTANCE);
        when(connection.serverAgent()).thenReturn("Neo4j/4.2.5");
        return connection;
    }
}
