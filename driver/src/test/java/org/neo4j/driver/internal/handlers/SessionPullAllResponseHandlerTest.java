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

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.Statement;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.messaging.v1.BoltProtocolV1.METADATA_EXTRACTOR;
import static org.neo4j.driver.v1.Values.value;

class SessionPullAllResponseHandlerTest
{
    @Test
    void shouldReleaseConnectionOnSuccess()
    {
        Connection connection = newConnectionMock();
        SessionPullAllResponseHandler handler = newHandler( connection );

        handler.onSuccess( emptyMap() );

        verify( connection ).release();
    }

    @Test
    void shouldReleaseConnectionOnFailure()
    {
        Connection connection = newConnectionMock();
        SessionPullAllResponseHandler handler = newHandler( connection );

        handler.onFailure( new RuntimeException() );

        verify( connection ).release();
    }

    @Test
    void shouldUpdateBookmarksOnSuccess()
    {
        String bookmarkValue = "neo4j:bookmark:v1:tx42";
        BookmarksHolder bookmarksHolder = mock( BookmarksHolder.class );
        SessionPullAllResponseHandler handler = newHandler( newConnectionMock(), bookmarksHolder );

        handler.onSuccess( singletonMap( "bookmark", value( bookmarkValue ) ) );

        verify( bookmarksHolder ).setBookmarks( Bookmarks.from( bookmarkValue ) );
    }

    private static SessionPullAllResponseHandler newHandler( Connection connection )
    {
        return newHandler( connection, BookmarksHolder.NO_OP );
    }

    private static SessionPullAllResponseHandler newHandler( Connection connection, BookmarksHolder bookmarksHolder )
    {
        RunResponseHandler runHandler = new RunResponseHandler( new CompletableFuture<>(), METADATA_EXTRACTOR );
        return new SessionPullAllResponseHandler( new Statement( "RETURN 1" ), runHandler, connection, bookmarksHolder, METADATA_EXTRACTOR );
    }

    private static Connection newConnectionMock()
    {
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( BoltServerAddress.LOCAL_DEFAULT );
        when( connection.serverVersion() ).thenReturn( ServerVersion.v3_2_0 );
        return connection;
    }
}
