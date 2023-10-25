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
package org.neo4j.driver.internal.handlers.pulln;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.SessionPullResponseCompletionListener;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.summary.ResultSummary;

class SessionPullResponseCompletionListenerTest extends BasicPullResponseHandlerTestBase {
    protected void shouldHandleSuccessWithSummary(BasicPullResponseHandler.State state) {
        // Given
        var conn = mockConnection();
        @SuppressWarnings("unchecked")
        BiConsumer<Record, Throwable> recordConsumer = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        BiConsumer<ResultSummary, Throwable> summaryConsumer = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        Consumer<DatabaseBookmark> bookmarksConsumer = mock(Consumer.class);
        PullResponseHandler handler =
                newSessionResponseHandler(conn, recordConsumer, summaryConsumer, bookmarksConsumer, state);

        // When
        handler.onSuccess(Collections.emptyMap());

        // Then
        verify(conn).release();
        verify(bookmarksConsumer).accept(any());
        verify(recordConsumer).accept(null, null);
        verify(summaryConsumer).accept(any(ResultSummary.class), eq(null));
    }

    @Override
    protected void shouldHandleFailure(BasicPullResponseHandler.State state) {
        // Given
        var conn = mockConnection();
        @SuppressWarnings("unchecked")
        BiConsumer<Record, Throwable> recordConsumer = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        BiConsumer<ResultSummary, Throwable> summaryConsumer = mock(BiConsumer.class);
        var handler = newResponseHandlerWithStatus(conn, recordConsumer, summaryConsumer, state);

        // When
        var error = new RuntimeException("I am an error");
        handler.onFailure(error);

        // Then
        assertThat(handler.state(), equalTo(BasicPullResponseHandler.State.FAILURE_STATE));
        verify(conn).release();
        verify(recordConsumer).accept(null, error);
        verify(summaryConsumer).accept(any(ResultSummary.class), eq(error));
    }

    @Override
    protected BasicPullResponseHandler newResponseHandlerWithStatus(
            Connection conn,
            BiConsumer<Record, Throwable> recordConsumer,
            BiConsumer<ResultSummary, Throwable> summaryConsumer,
            BasicPullResponseHandler.State state) {
        return newSessionResponseHandler(conn, recordConsumer, summaryConsumer, (ignored) -> {}, state);
    }

    private static BasicPullResponseHandler newSessionResponseHandler(
            Connection conn,
            BiConsumer<Record, Throwable> recordConsumer,
            BiConsumer<ResultSummary, Throwable> summaryConsumer,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            BasicPullResponseHandler.State state) {
        var runHandler = mock(RunResponseHandler.class);
        var listener = new SessionPullResponseCompletionListener(conn, bookmarkConsumer);
        var handler = new BasicPullResponseHandler(
                mock(Query.class), runHandler, conn, BoltProtocolV4.METADATA_EXTRACTOR, listener);

        handler.installRecordConsumer(recordConsumer);
        handler.installSummaryConsumer(summaryConsumer);

        handler.state(state);
        return handler;
    }
}
