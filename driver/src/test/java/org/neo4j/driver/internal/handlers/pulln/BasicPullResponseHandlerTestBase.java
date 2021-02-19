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
package org.neo4j.driver.internal.handlers.pulln;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.messaging.request.DiscardMessage;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.ResultSummary;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

abstract class BasicPullResponseHandlerTestBase
{
    protected abstract void shouldHandleSuccessWithSummary( BasicPullResponseHandler.State state );

    protected abstract void shouldHandleFailure( BasicPullResponseHandler.State state );

    protected abstract BasicPullResponseHandler newResponseHandlerWithStatus( Connection conn, BiConsumer<Record,Throwable> recordConsumer,
                                                                              BiConsumer<ResultSummary,Throwable> summaryConsumer,
                                                                              BasicPullResponseHandler.State state );

    // on success with summary
    @ParameterizedTest
    @MethodSource( "allStatus" )
    void shouldSuccessWithSummary( BasicPullResponseHandler.State state ) throws Throwable
    {
        shouldHandleSuccessWithSummary( state );
    }

    // on success with has_more
    @Test
    void shouldRequestMoreWithHasMore() throws Throwable
    {
        // Given a handler in streaming state
        Connection conn = mockConnection();
        BasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, BasicPullResponseHandler.State.STREAMING_STATE );

        // When
        handler.request( 100 ); // I append a request to ask for more

        handler.onSuccess( metaWithHasMoreEqualsTrue() );

        // Then
        verify( conn ).writeAndFlush( any( PullMessage.class ), eq( handler ) );
        assertThat( handler.state(), equalTo( BasicPullResponseHandler.State.STREAMING_STATE ) );
    }

    @Test
    void shouldInformSummaryConsumerSuccessWithHasMore() throws Throwable
    {
        // Given
        Connection conn = mockConnection();
        BiConsumer<Record,Throwable> recordConsumer = mock( BiConsumer.class );
        BiConsumer<ResultSummary,Throwable> summaryConsumer = mock( BiConsumer.class );
        BasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, recordConsumer, summaryConsumer, BasicPullResponseHandler.State.STREAMING_STATE );
        // When

        handler.onSuccess( metaWithHasMoreEqualsTrue() );

        // Then
        verifyNoMoreInteractions( conn );
        verifyNoMoreInteractions( recordConsumer );
        verify( summaryConsumer ).accept( null, null );
        assertThat( handler.state(), equalTo( BasicPullResponseHandler.State.READY_STATE ) );
    }

    @Test
    void shouldDiscardIfStreamingIsCanceled() throws Throwable
    {
        // Given a handler in streaming state
        Connection conn = mockConnection();
        BasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, BasicPullResponseHandler.State.CANCELLED_STATE );
        handler.onSuccess( metaWithHasMoreEqualsTrue() );

        // Then
        verify( conn ).writeAndFlush( any( DiscardMessage.class ), eq( handler ) );
        assertThat( handler.state(), equalTo( BasicPullResponseHandler.State.CANCELLED_STATE ) );
    }

    // on failure
    @ParameterizedTest
    @MethodSource( "allStatus" )
    void shouldErrorToRecordAndSummaryConsumer( BasicPullResponseHandler.State state ) throws Throwable
    {
        shouldHandleFailure( state );
    }

    // on record
    @Test
    void shouldReportRecordInStreaming() throws Throwable
    {
        // Given a handler in streaming state
        Connection conn = mockConnection();
        BiConsumer<Record,Throwable> recordConsumer = mock( BiConsumer.class );
        BiConsumer<ResultSummary,Throwable> summaryConsumer = mock( BiConsumer.class );
        BasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, recordConsumer, summaryConsumer, BasicPullResponseHandler.State.STREAMING_STATE );

        // When
        handler.onRecord( new Value[0] );

        // Then
        verify( recordConsumer ).accept( any( Record.class ), eq( null ) );
        verifyNoMoreInteractions( summaryConsumer );
        verifyNoMoreInteractions( conn );
        assertThat( handler.state(), equalTo( BasicPullResponseHandler.State.STREAMING_STATE ) );
    }

    @ParameterizedTest
    @MethodSource( "allStatusExceptStreaming" )
    void shouldNotReportRecordWhenNotStreaming( BasicPullResponseHandler.State state ) throws Throwable
    {
        // Given a handler in streaming state
        Connection conn = mockConnection();
        BiConsumer<Record,Throwable> recordConsumer = mock( BiConsumer.class );
        BiConsumer<ResultSummary,Throwable> summaryConsumer = mock( BiConsumer.class );
        BasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, recordConsumer, summaryConsumer, state );

        // When
        handler.onRecord( new Value[0] );

        // Then
        verifyNoMoreInteractions( recordConsumer );
        verifyNoMoreInteractions( summaryConsumer );
        assertThat( handler.state(), equalTo( state ) );
    }

    // request
    @Test
    void shouldStayInStreaming() throws Throwable
    {
        // Given
        Connection conn = mockConnection();
        BasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, BasicPullResponseHandler.State.STREAMING_STATE );

        // When
        handler.request( 100 );

        // Then
        assertThat( handler.state(), equalTo( BasicPullResponseHandler.State.STREAMING_STATE ) );
    }

    @Test
    void shouldPullAndSwitchStreamingInReady() throws Throwable
    {
        // Given
        Connection conn = mockConnection();
        BasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, BasicPullResponseHandler.State.READY_STATE );

        // When
        handler.request( 100 );

        // Then
        verify( conn ).writeAndFlush( any( PullMessage.class ), eq( handler ) );
        assertThat( handler.state(), equalTo( BasicPullResponseHandler.State.STREAMING_STATE ) );
    }

    // cancel
    @Test
    void shouldStayInCancel() throws Throwable
    {
        // Given
        Connection conn = mockConnection();
        BasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, BasicPullResponseHandler.State.CANCELLED_STATE );

        // When
        handler.cancel();

        // Then
        verifyNoMoreInteractions( conn );
        assertThat( handler.state(), equalTo( BasicPullResponseHandler.State.CANCELLED_STATE ) );
    }

    @Test
    void shouldSwitchFromStreamingToCancel() throws Throwable
    {
        // Given
        Connection conn = mockConnection();
        BasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, BasicPullResponseHandler.State.STREAMING_STATE );

        // When
        handler.cancel();

        // Then
        verifyNoMoreInteractions( conn );
        assertThat( handler.state(), equalTo( BasicPullResponseHandler.State.CANCELLED_STATE ) );
    }

    @Test
    void shouldSwitchFromReadyToCancel() throws Throwable
    {
        // Given
        Connection conn = mockConnection();
        BasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, BasicPullResponseHandler.State.READY_STATE );

        // When
        handler.cancel();

        // Then
        verify( conn ).writeAndFlush( any( DiscardMessage.class ), eq( handler ) );
        assertThat( handler.state(), equalTo( BasicPullResponseHandler.State.CANCELLED_STATE ) );
    }

    static Connection mockConnection()
    {
        Connection conn = mock( Connection.class );
        when( conn.serverAddress() ).thenReturn( mock( BoltServerAddress.class ) );
        when( conn.serverVersion() ).thenReturn( mock( ServerVersion.class ) );
        return conn;
    }

    private BasicPullResponseHandler newResponseHandlerWithStatus( Connection conn, BasicPullResponseHandler.State state )
    {
        BiConsumer<Record,Throwable> recordConsumer = mock( BiConsumer.class );
        BiConsumer<ResultSummary,Throwable> summaryConsumer = mock( BiConsumer.class );
        return newResponseHandlerWithStatus( conn, recordConsumer, summaryConsumer, state );
    }

    private static HashMap<String,Value> metaWithHasMoreEqualsTrue()
    {
        HashMap<String,Value> meta = new HashMap<>( 1 );
        meta.put( "has_more", BooleanValue.TRUE );
        return meta;
    }

    private static Stream<BasicPullResponseHandler.State> allStatusExceptStreaming()
    {
        return Stream.of( BasicPullResponseHandler.State.SUCCEEDED_STATE, BasicPullResponseHandler.State.FAILURE_STATE,
                          BasicPullResponseHandler.State.CANCELLED_STATE, BasicPullResponseHandler.State.READY_STATE );
    }

    private static Stream<BasicPullResponseHandler.State> allStatus()
    {
        return Stream.of( BasicPullResponseHandler.State.SUCCEEDED_STATE, BasicPullResponseHandler.State.FAILURE_STATE,
                          BasicPullResponseHandler.State.CANCELLED_STATE, BasicPullResponseHandler.State.READY_STATE,
                          BasicPullResponseHandler.State.STREAMING_STATE );
    }
}
