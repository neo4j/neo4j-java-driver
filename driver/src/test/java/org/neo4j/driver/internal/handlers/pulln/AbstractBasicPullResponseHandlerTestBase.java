/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler.Status;
import org.neo4j.driver.internal.messaging.request.DiscardNMessage;
import org.neo4j.driver.internal.messaging.request.PullNMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.ResultSummary;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler.Status.CANCELED;
import static org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler.Status.DONE;
import static org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler.Status.FAILED;
import static org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler.Status.READY;
import static org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler.Status.STREAMING;

abstract class AbstractBasicPullResponseHandlerTestBase
{
    protected abstract void shouldHandleSuccessWithSummary( Status status );
    protected abstract void shouldHandleFailure( Status status );
    protected abstract AbstractBasicPullResponseHandler newResponseHandlerWithStatus( Connection conn, BiConsumer<Record,Throwable> recordConsumer,
            BiConsumer<ResultSummary,Throwable> summaryConsumer, Status status );

    // on success with summary
    @ParameterizedTest
    @MethodSource( "allStatus" )
    void shouldSuccessWithSummary( Status status ) throws Throwable
    {
        shouldHandleSuccessWithSummary( status );
    }

    // on success with has_more
    @Test
    void shouldRequestMoreWithHasMore() throws Throwable
    {
        // Given a handler in streaming state
        Connection conn = mockConnection();
        AbstractBasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, STREAMING );

        // When
        handler.request( 100 ); // I append a request to ask for more

        handler.onSuccess( metaWithHasMoreEqualsTrue() );

        // Then
        verify( conn ).writeAndFlush( any( PullNMessage.class ), eq( handler ) );
        assertThat( handler.status(), equalTo( STREAMING ) );
    }

    @Test
    void shouldInformSummaryConsumerSuccessWithHasMore() throws Throwable
    {
        // Given
        Connection conn = mockConnection();
        BiConsumer<Record,Throwable> recordConsumer = mock( BiConsumer.class );
        BiConsumer<ResultSummary,Throwable> summaryConsumer = mock( BiConsumer.class );
        AbstractBasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, recordConsumer, summaryConsumer, STREAMING );
        // When

        handler.onSuccess( metaWithHasMoreEqualsTrue() );

        // Then
        verifyNoMoreInteractions( conn );
        verifyNoMoreInteractions( recordConsumer );
        verify( summaryConsumer ).accept( null, null );
        assertThat( handler.status(), equalTo( READY ) );
    }

    @Test
    void shouldDiscardIfStreamingIsCanceled() throws Throwable
    {
        // Given a handler in streaming state
        Connection conn = mockConnection();
        AbstractBasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, CANCELED );
        handler.onSuccess( metaWithHasMoreEqualsTrue() );

        // Then
        verify( conn ).writeAndFlush( any( DiscardNMessage.class ), eq( handler ) );
        assertThat( handler.status(), equalTo( CANCELED ) );
    }

    // on failure
    @ParameterizedTest
    @MethodSource( "allStatus" )
    void shouldErrorToRecordAndSummaryConsumer( Status status ) throws Throwable
    {
        shouldHandleFailure( status );
    }

    // on record
    @Test
    void shouldReportRecordInStreaming() throws Throwable
    {
        // Given a handler in streaming state
        Connection conn = mockConnection();
        BiConsumer<Record,Throwable> recordConsumer = mock( BiConsumer.class );
        BiConsumer<ResultSummary,Throwable> summaryConsumer = mock( BiConsumer.class );
        AbstractBasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, recordConsumer, summaryConsumer, STREAMING );

        // When
        handler.onRecord( new Value[0] );

        // Then
        verify( recordConsumer ).accept( any( Record.class ), eq( null ) );
        verifyNoMoreInteractions( summaryConsumer );
        verifyNoMoreInteractions( conn );
        assertThat( handler.status(), equalTo( STREAMING ) );
    }

    @ParameterizedTest
    @MethodSource( "allStatusExceptStreaming" )
    void shouldNotReportRecordWhenNotStreaming( Status status ) throws Throwable
    {
        // Given a handler in streaming state
        Connection conn = mockConnection();
        BiConsumer<Record,Throwable> recordConsumer = mock( BiConsumer.class );
        BiConsumer<ResultSummary,Throwable> summaryConsumer = mock( BiConsumer.class );
        AbstractBasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, recordConsumer, summaryConsumer, status );

        // When
        handler.onRecord( new Value[0] );

        // Then
        verifyNoMoreInteractions( recordConsumer );
        verifyNoMoreInteractions( summaryConsumer );
        assertThat( handler.status(), equalTo( status ) );
    }

    // request
    @Test
    void shouldStayInStreaming() throws Throwable
    {
        // Given
        Connection conn = mockConnection();
        AbstractBasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, STREAMING );

        // When
        handler.request( 100 );

        // Then
        assertThat( handler.status(), equalTo( STREAMING ) );
    }

    @Test
    void shouldPullAndSwitchStreamingInReady() throws Throwable
    {
        // Given
        Connection conn = mockConnection();
        AbstractBasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, READY );

        // When
        handler.request( 100 );

        // Then
        verify( conn ).writeAndFlush( any( PullNMessage.class ), eq( handler ) );
        assertThat( handler.status(), equalTo( STREAMING ) );
    }

    // cancel
    @Test
    void shouldStayInCancel() throws Throwable
    {
        // Given
        Connection conn = mockConnection();
        AbstractBasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, CANCELED );

        // When
        handler.cancel();

        // Then
        verifyNoMoreInteractions( conn );
        assertThat( handler.status(), equalTo( CANCELED ) );
    }

    @Test
    void shouldSwitchFromStreamingToCancel() throws Throwable
    {
        // Given
        Connection conn = mockConnection();
        AbstractBasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, STREAMING );

        // When
        handler.cancel();

        // Then
        verifyNoMoreInteractions( conn );
        assertThat( handler.status(), equalTo( CANCELED ) );
    }

    @Test
    void shouldSwitchFromReadyToCancel() throws Throwable
    {
        // Given
        Connection conn = mockConnection();
        AbstractBasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, READY );

        // When
        handler.cancel();

        // Then
        verify( conn ).writeAndFlush( any( DiscardNMessage.class ), eq( handler ) );
        assertThat( handler.status(), equalTo( CANCELED ) );
    }

    static Connection mockConnection()
    {
        Connection conn = mock( Connection.class );
        when( conn.serverAddress() ).thenReturn( mock( BoltServerAddress.class ) );
        when( conn.serverVersion() ).thenReturn( mock( ServerVersion.class ) );
        return conn;
    }

    private AbstractBasicPullResponseHandler newResponseHandlerWithStatus( Connection conn, Status status )
    {
        BiConsumer<Record,Throwable> recordConsumer = mock( BiConsumer.class );
        BiConsumer<ResultSummary,Throwable> summaryConsumer = mock( BiConsumer.class );
        return newResponseHandlerWithStatus( conn, recordConsumer, summaryConsumer, status );
    }

    private static HashMap<String,Value> metaWithHasMoreEqualsTrue()
    {
        HashMap<String,Value> meta = new HashMap<>( 1 );
        meta.put( "has_more", BooleanValue.TRUE );
        return meta;
    }

    private static Stream<Status> allStatusExceptStreaming()
    {
        return Stream.of( DONE, FAILED, CANCELED, READY );
    }

    private static Stream<Status> allStatus()
    {
        return Stream.of( DONE, FAILED, CANCELED, STREAMING, READY );
    }
}
