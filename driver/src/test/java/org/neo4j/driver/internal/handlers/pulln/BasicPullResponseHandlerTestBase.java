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
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.messaging.request.DiscardMessage;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.messaging.v43.BoltProtocolV43;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.summary.ResultSummary;

abstract class BasicPullResponseHandlerTestBase {
    protected abstract void shouldHandleSuccessWithSummary(BasicPullResponseHandler.State state);

    protected abstract void shouldHandleFailure(BasicPullResponseHandler.State state);

    protected abstract BasicPullResponseHandler newResponseHandlerWithStatus(
            Connection conn,
            BiConsumer<Record, Throwable> recordConsumer,
            BiConsumer<ResultSummary, Throwable> summaryConsumer,
            BasicPullResponseHandler.State state);

    // on success with summary
    @ParameterizedTest
    @MethodSource("allStatus")
    void shouldSuccessWithSummary(BasicPullResponseHandler.State state) {
        shouldHandleSuccessWithSummary(state);
    }

    // on success with has_more
    @Test
    void shouldRequestMoreWithHasMore() {
        // Given a handler in streaming state
        var conn = mockConnection();
        var handler = newResponseHandlerWithStatus(conn, BasicPullResponseHandler.State.STREAMING_STATE);

        // When
        handler.request(100); // I append a request to ask for more

        handler.onSuccess(metaWithHasMoreEqualsTrue());

        // Then
        verify(conn).writeAndFlush(any(PullMessage.class), eq(handler));
        assertThat(handler.state(), equalTo(BasicPullResponseHandler.State.STREAMING_STATE));
    }

    @Test
    void shouldInformSummaryConsumerSuccessWithHasMore() {
        // Given
        var conn = mockConnection();
        @SuppressWarnings("unchecked")
        BiConsumer<Record, Throwable> recordConsumer = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        BiConsumer<ResultSummary, Throwable> summaryConsumer = mock(BiConsumer.class);
        var handler = newResponseHandlerWithStatus(
                conn, recordConsumer, summaryConsumer, BasicPullResponseHandler.State.STREAMING_STATE);
        // When

        handler.onSuccess(metaWithHasMoreEqualsTrue());

        // Then
        verifyNoMoreInteractions(conn);
        verifyNoMoreInteractions(recordConsumer);
        verify(summaryConsumer).accept(null, null);
        assertThat(handler.state(), equalTo(BasicPullResponseHandler.State.READY_STATE));
    }

    @Test
    void shouldDiscardIfStreamingIsCanceled() {
        // Given a handler in streaming state
        var conn = mockConnection();
        var handler = newResponseHandlerWithStatus(conn, BasicPullResponseHandler.State.CANCELLED_STATE);
        handler.onSuccess(metaWithHasMoreEqualsTrue());

        // Then
        verify(conn).writeAndFlush(any(DiscardMessage.class), eq(handler));
        assertThat(handler.state(), equalTo(BasicPullResponseHandler.State.CANCELLED_STATE));
    }

    // on failure
    @ParameterizedTest
    @MethodSource("allStatus")
    void shouldErrorToRecordAndSummaryConsumer(BasicPullResponseHandler.State state) {
        shouldHandleFailure(state);
    }

    // on record
    @Test
    void shouldReportRecordInStreaming() {
        // Given a handler in streaming state
        var conn = mockConnection();
        @SuppressWarnings("unchecked")
        BiConsumer<Record, Throwable> recordConsumer = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        BiConsumer<ResultSummary, Throwable> summaryConsumer = mock(BiConsumer.class);
        var handler = newResponseHandlerWithStatus(
                conn, recordConsumer, summaryConsumer, BasicPullResponseHandler.State.STREAMING_STATE);

        // When
        handler.onRecord(new Value[0]);

        // Then
        verify(recordConsumer).accept(any(Record.class), eq(null));
        verifyNoMoreInteractions(summaryConsumer);
        verifyNoMoreInteractions(conn);
        assertThat(handler.state(), equalTo(BasicPullResponseHandler.State.STREAMING_STATE));
    }

    @ParameterizedTest
    @MethodSource("allStatusExceptStreaming")
    void shouldNotReportRecordWhenNotStreaming(BasicPullResponseHandler.State state) {
        // Given a handler in streaming state
        var conn = mockConnection();
        @SuppressWarnings("unchecked")
        BiConsumer<Record, Throwable> recordConsumer = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        BiConsumer<ResultSummary, Throwable> summaryConsumer = mock(BiConsumer.class);
        var handler = newResponseHandlerWithStatus(conn, recordConsumer, summaryConsumer, state);

        // When
        handler.onRecord(new Value[0]);

        // Then
        verifyNoMoreInteractions(recordConsumer);
        verifyNoMoreInteractions(summaryConsumer);
        assertThat(handler.state(), equalTo(state));
    }

    // request
    @Test
    void shouldStayInStreaming() {
        // Given
        var conn = mockConnection();
        var handler = newResponseHandlerWithStatus(conn, BasicPullResponseHandler.State.STREAMING_STATE);

        // When
        handler.request(100);

        // Then
        assertThat(handler.state(), equalTo(BasicPullResponseHandler.State.STREAMING_STATE));
    }

    @Test
    void shouldPullAndSwitchStreamingInReady() {
        // Given
        var conn = mockConnection();
        var handler = newResponseHandlerWithStatus(conn, BasicPullResponseHandler.State.READY_STATE);

        // When
        handler.request(100);

        // Then
        verify(conn).writeAndFlush(any(PullMessage.class), eq(handler));
        assertThat(handler.state(), equalTo(BasicPullResponseHandler.State.STREAMING_STATE));
    }

    // cancel
    @Test
    void shouldStayInCancel() {
        // Given
        var conn = mockConnection();
        var handler = newResponseHandlerWithStatus(conn, BasicPullResponseHandler.State.CANCELLED_STATE);

        // When
        handler.cancel();

        // Then
        verifyNoMoreInteractions(conn);
        assertThat(handler.state(), equalTo(BasicPullResponseHandler.State.CANCELLED_STATE));
    }

    @Test
    void shouldSwitchFromStreamingToCancel() {
        // Given
        var conn = mockConnection();
        var handler = newResponseHandlerWithStatus(conn, BasicPullResponseHandler.State.STREAMING_STATE);

        // When
        handler.cancel();

        // Then
        verifyNoMoreInteractions(conn);
        assertThat(handler.state(), equalTo(BasicPullResponseHandler.State.CANCELLED_STATE));
    }

    @Test
    void shouldSwitchFromReadyToCancel() {
        // Given
        var conn = mockConnection();
        var handler = newResponseHandlerWithStatus(conn, BasicPullResponseHandler.State.READY_STATE);

        // When
        handler.cancel();

        // Then
        verify(conn).writeAndFlush(any(DiscardMessage.class), eq(handler));
        assertThat(handler.state(), equalTo(BasicPullResponseHandler.State.CANCELLED_STATE));
    }

    static Connection mockConnection() {
        var conn = mock(Connection.class);
        when(conn.serverAddress()).thenReturn(mock(BoltServerAddress.class));
        when(conn.protocol()).thenReturn(BoltProtocolV43.INSTANCE);
        when(conn.serverAgent()).thenReturn("Neo4j/4.2.5");
        return conn;
    }

    private BasicPullResponseHandler newResponseHandlerWithStatus(
            Connection conn, BasicPullResponseHandler.State state) {
        @SuppressWarnings("unchecked")
        BiConsumer<Record, Throwable> recordConsumer = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        BiConsumer<ResultSummary, Throwable> summaryConsumer = mock(BiConsumer.class);
        return newResponseHandlerWithStatus(conn, recordConsumer, summaryConsumer, state);
    }

    private static HashMap<String, Value> metaWithHasMoreEqualsTrue() {
        var meta = new HashMap<String, Value>(1);
        meta.put("has_more", BooleanValue.TRUE);
        return meta;
    }

    private static Stream<BasicPullResponseHandler.State> allStatusExceptStreaming() {
        return Stream.of(
                BasicPullResponseHandler.State.SUCCEEDED_STATE, BasicPullResponseHandler.State.FAILURE_STATE,
                BasicPullResponseHandler.State.CANCELLED_STATE, BasicPullResponseHandler.State.READY_STATE);
    }

    private static Stream<BasicPullResponseHandler.State> allStatus() {
        return Stream.of(
                BasicPullResponseHandler.State.SUCCEEDED_STATE,
                BasicPullResponseHandler.State.FAILURE_STATE,
                BasicPullResponseHandler.State.CANCELLED_STATE,
                BasicPullResponseHandler.State.READY_STATE,
                BasicPullResponseHandler.State.STREAMING_STATE);
    }
}
