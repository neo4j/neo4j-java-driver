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
package org.neo4j.driver.internal.handlers;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.Query;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.util.TestUtil.anyServerVersion;

class SessionPullResponseCompletionListenerTest
{
    @Test
    void shouldReleaseConnectionOnSuccess()
    {
        Connection connection = newConnectionMock();
        PullResponseCompletionListener listener = new SessionPullResponseCompletionListener( connection, BookmarkHolder.NO_OP );
        ResponseHandler handler = newHandler( connection, listener );

        handler.onSuccess( emptyMap() );

        verify( connection ).release();
    }

    @Test
    void shouldReleaseConnectionOnFailure()
    {
        Connection connection = newConnectionMock();
        PullResponseCompletionListener listener = new SessionPullResponseCompletionListener( connection, BookmarkHolder.NO_OP );
        ResponseHandler handler = newHandler( connection, listener );

        handler.onFailure( new RuntimeException() );

        verify( connection ).release();
    }

    @Test
    void shouldUpdateBookmarksOnSuccess()
    {
        Connection connection = newConnectionMock();
        String bookmarkValue = "neo4j:bookmark:v1:tx42";
        BookmarkHolder bookmarkHolder = mock( BookmarkHolder.class );
        PullResponseCompletionListener listener = new SessionPullResponseCompletionListener( connection, bookmarkHolder );
        ResponseHandler handler = newHandler( connection, listener );

        handler.onSuccess( singletonMap( "bookmark", value( bookmarkValue ) ) );

        verify( bookmarkHolder ).setBookmark( InternalBookmark.parse( bookmarkValue ) );
    }

    private static ResponseHandler newHandler( Connection connection, PullResponseCompletionListener listener )
    {
        RunResponseHandler runHandler = new RunResponseHandler( new CompletableFuture<>(), BoltProtocolV3.METADATA_EXTRACTOR );
        BasicPullResponseHandler handler =
                new BasicPullResponseHandler( new Query( "RETURN 1" ), runHandler, connection, BoltProtocolV3.METADATA_EXTRACTOR, listener );
        handler.installRecordConsumer( ( record, throwable ) -> {} );
        handler.installSummaryConsumer( ( resultSummary, throwable ) -> {} );
        return handler;
    }

    private static Connection newConnectionMock()
    {
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( BoltServerAddress.LOCAL_DEFAULT );
        when( connection.serverVersion() ).thenReturn( anyServerVersion() );
        return connection;
    }
}
