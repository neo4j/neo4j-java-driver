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
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.function.BiConsumer;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.TransactionPullResponseCompletionListener;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.summary.ResultSummary;

public class TransactionPullResponseCompletionListenerTest extends BasicPullResponseHandlerTestBase {
    @Override
    protected void shouldHandleSuccessWithSummary(BasicPullResponseHandler.State state) {
        // Given
        var conn = mockConnection();
        @SuppressWarnings("unchecked")
        BiConsumer<Record, Throwable> recordConsumer = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        BiConsumer<ResultSummary, Throwable> summaryConsumer = mock(BiConsumer.class);
        var handler = newResponseHandlerWithStatus(conn, recordConsumer, summaryConsumer, state);

        // When
        handler.onSuccess(Collections.emptyMap());

        // Then
        assertThat(handler.state(), equalTo(BasicPullResponseHandler.State.SUCCEEDED_STATE));
        verify(recordConsumer).accept(null, null);
        verify(summaryConsumer).accept(any(ResultSummary.class), eq(null));
    }

    @Override
    @SuppressWarnings("ThrowableNotThrown")
    protected void shouldHandleFailure(BasicPullResponseHandler.State state) {
        // Given
        var conn = mockConnection();
        @SuppressWarnings("unchecked")
        BiConsumer<Record, Throwable> recordConsumer = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        BiConsumer<ResultSummary, Throwable> summaryConsumer = mock(BiConsumer.class);
        var tx = mock(UnmanagedTransaction.class);
        when(tx.isOpen()).thenReturn(true);
        var handler = newTxResponseHandler(conn, recordConsumer, summaryConsumer, tx, state);

        // When
        var error = new RuntimeException("I am an error");
        handler.onFailure(error);

        // Then
        assertThat(handler.state(), equalTo(BasicPullResponseHandler.State.FAILURE_STATE));
        verify(tx).markTerminated(error);
        verify(recordConsumer).accept(null, error);
        verify(summaryConsumer).accept(any(ResultSummary.class), eq(error));
    }

    @Override
    protected BasicPullResponseHandler newResponseHandlerWithStatus(
            Connection conn,
            BiConsumer<Record, Throwable> recordConsumer,
            BiConsumer<ResultSummary, Throwable> summaryConsumer,
            BasicPullResponseHandler.State state) {
        var tx = mock(UnmanagedTransaction.class);
        return newTxResponseHandler(conn, recordConsumer, summaryConsumer, tx, state);
    }

    private static BasicPullResponseHandler newTxResponseHandler(
            Connection conn,
            BiConsumer<Record, Throwable> recordConsumer,
            BiConsumer<ResultSummary, Throwable> summaryConsumer,
            UnmanagedTransaction tx,
            BasicPullResponseHandler.State state) {
        var runHandler = mock(RunResponseHandler.class);
        var listener = new TransactionPullResponseCompletionListener(tx);
        var handler = new BasicPullResponseHandler(
                mock(Query.class), runHandler, conn, BoltProtocolV4.METADATA_EXTRACTOR, listener);

        handler.installRecordConsumer(recordConsumer);
        handler.installSummaryConsumer(summaryConsumer);

        handler.state(state);
        return handler;
    }
}
