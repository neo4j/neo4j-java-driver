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
package org.neo4j.driver.reactive.internal.cursor;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.AbstractBasicPullResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;

import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.messaging.v3.BoltProtocolV3.METADATA_EXTRACTOR;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.ServerVersion.v4_0_0;
import static org.neo4j.driver.v1.Values.value;

class RxStatementResultCursorTest
{
    @Test
    void shouldWaitForRunToFinishBeforeCreatingRxResultCurosr() throws Throwable
    {
        // Given
        CompletableFuture<Throwable> runFuture = new CompletableFuture<>();
        RunResponseHandler runHandler = newRunResponseHandler( runFuture );
        BasicPullResponseHandler pullHandler = mock( BasicPullResponseHandler.class );

        // When
        IllegalStateException error = assertThrows( IllegalStateException.class, () -> new RxStatementResultCursor( runHandler, pullHandler ) );
        // Then
        assertThat( error.getMessage(), containsString( "Should wait for response of RUN" ) );
    }

    @Test
    void shouldReturnStatementKeys() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        List<String> expected = asList( "key1", "key2", "key3" );
        runHandler.onSuccess( Collections.singletonMap( "fields", value( expected ) ) );

        BasicPullResponseHandler pullHandler = mock( BasicPullResponseHandler.class );

        // When
        RxStatementResultCursor cursor = new RxStatementResultCursor( runHandler, pullHandler );
        List<String> actual = cursor.keys();

        // Then
        assertEquals( expected, actual );
    }

    @Test
    void shouldSupportReturnStatementKeysMultipleTimes() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        List<String> expected = asList( "key1", "key2", "key3" );
        runHandler.onSuccess( Collections.singletonMap( "fields", value( expected ) ) );

        BasicPullResponseHandler pullHandler = mock( BasicPullResponseHandler.class );

        // When
        RxStatementResultCursor cursor = new RxStatementResultCursor( runHandler, pullHandler );

        // Then
        List<String> actual = cursor.keys();
        assertEquals( expected, actual );

        // Many times
        actual = cursor.keys();
        assertEquals( expected, actual );

        actual = cursor.keys();
        assertEquals( expected, actual );
    }

    @Test
    void shouldSendPullReequestToPullHandler() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        BasicPullResponseHandler pullHandler = mock( BasicPullResponseHandler.class );
        RxStatementResultCursor cursor = new RxStatementResultCursor( runHandler, pullHandler );

        // When
        cursor.request( 100 );

        // Then
        verify( pullHandler ).request( 100 );
    }

    @Test
    void runErrorShouldFailPullHandlerWhenCancel() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        BasicPullResponseHandler pullHandler = mock( BasicPullResponseHandler.class );
        RxStatementResultCursor cursor = new RxStatementResultCursor( runHandler, pullHandler );

        // When
        cursor.cancel();

        // Then
        verify( pullHandler ).cancel();
    }

    @Test
    void shouldInstallSummaryConsumerWithoutReportingError() throws Throwable
    {
        // Given
        RuntimeException error = new RuntimeException( "Hi" );
        RunResponseHandler runHandler = newRunResponseHandler( error );
        BasicPullResponseHandler pullHandler = mock( BasicPullResponseHandler.class );
        RxStatementResultCursor cursor = new RxStatementResultCursor( error, runHandler, pullHandler );

        // When
        BiConsumer summaryConsumer = mock( BiConsumer.class );
        cursor.installSummaryConsumer( summaryConsumer );

        // Then
        verify( pullHandler ).installSummaryConsumer( summaryConsumer );
        verifyNoMoreInteractions( pullHandler );
    }

    @Test
    void shouldInstallRecordConsumerAndReportError() throws Throwable
    {
        // Given
        RuntimeException error = new RuntimeException( "Hi" );
        BiConsumer recordConsumer = mock( BiConsumer.class );

        // When
        newCursor( error, recordConsumer );

        // Then
        verify( recordConsumer ).accept( null, error );
        verifyNoMoreInteractions( recordConsumer );
    }

    private static RunResponseHandler newRunResponseHandler( CompletableFuture<Throwable> runFuture )
    {
        return new RunResponseHandler( runFuture, METADATA_EXTRACTOR );
    }

    private static RunResponseHandler newRunResponseHandler( Throwable error )
    {
        return newRunResponseHandler( completedFuture( error ) );
    }

    private static RunResponseHandler newRunResponseHandler()
    {
        return newRunResponseHandler( completedWithNull() );
    }

    private static BasicPullResponseHandler newPullResponseHandler( RunResponseHandler runHandler )
    {
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( LOCAL_DEFAULT );
        when( connection.serverVersion() ).thenReturn( v4_0_0 );
        AbstractBasicPullResponseHandler pullHandler =
                new AbstractBasicPullResponseHandler( new Statement( "Any statement" ), runHandler, connection, METADATA_EXTRACTOR )
                {
                    @Override
                    protected void afterSuccess( Map<String,Value> metadata )
                    {

                    }

                    @Override
                    protected void afterFailure( Throwable error )
                    {

                    }
                };
        pullHandler.installSummaryConsumer( ( s, e ) -> {
        } );
        return pullHandler;
    }

    private static RxStatementResultCursor newCursor( Throwable error, BiConsumer<Record,Throwable> recordConsumer )
    {
        RunResponseHandler runHandler = newRunResponseHandler( error );
        BasicPullResponseHandler pullHandler = newPullResponseHandler( runHandler );
        RxStatementResultCursor cursor = new RxStatementResultCursor( error, runHandler, pullHandler );
        cursor.installRecordConsumer( recordConsumer );
        return cursor;
    }
}
