/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.driver.internal.messaging.v1;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.cursor.AsyncResultCursorOnlyFactory;
import org.neo4j.driver.internal.cursor.ResultCursorFactory;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.InitResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.PullHandlers;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.InitMessage;
import org.neo4j.driver.internal.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.MetadataExtractor;

import static java.util.Collections.emptyMap;
import static org.neo4j.driver.Values.ofValue;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.assertEmptyDatabaseName;
import static org.neo4j.driver.internal.util.Iterables.newHashMapWithSize;

public class BoltProtocolV1 implements BoltProtocol
{
    public static final int VERSION = 1;

    public static final BoltProtocol INSTANCE = new BoltProtocolV1();

    public static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor( "result_available_after", "result_consumed_after" );

    private static final String BEGIN_QUERY = "BEGIN";
    private static final Message BEGIN_MESSAGE = new RunMessage( BEGIN_QUERY );
    private static final Message COMMIT_MESSAGE = new RunMessage( "COMMIT" );
    private static final Message ROLLBACK_MESSAGE = new RunMessage( "ROLLBACK" );

    @Override
    public MessageFormat createMessageFormat()
    {
        return new MessageFormatV1();
    }

    @Override
    public void initializeChannel( String userAgent, Map<String,Value> authToken, ChannelPromise channelInitializedPromise )
    {
        Channel channel = channelInitializedPromise.channel();

        InitMessage message = new InitMessage( userAgent, authToken );
        InitResponseHandler handler = new InitResponseHandler( channelInitializedPromise );

        messageDispatcher( channel ).enqueue( handler );
        channel.writeAndFlush( message, channel.voidPromise() );
    }

    @Override
    public void prepareToCloseChannel( Channel channel )
    {
        // left empty on purpose.
    }

    @Override
    public CompletionStage<Void> beginTransaction( Connection connection, Bookmark bookmark, TransactionConfig config )
    {
        try
        {
            verifyBeforeTransaction( config, connection.databaseName() );
        }
        catch ( Exception error )
        {
            return Futures.failedFuture( error );
        }

        if ( bookmark.isEmpty() )
        {
            connection.write(
                    BEGIN_MESSAGE, NoOpResponseHandler.INSTANCE,
                    PullAllMessage.PULL_ALL, NoOpResponseHandler.INSTANCE );

            return Futures.completedWithNull();
        }
        else
        {
            CompletableFuture<Void> beginTxFuture = new CompletableFuture<>();
            connection.writeAndFlush(
                    new RunMessage( BEGIN_QUERY, SingleBookmarkHelper.asBeginTransactionParameters( bookmark ) ), NoOpResponseHandler.INSTANCE,
                    PullAllMessage.PULL_ALL, new BeginTxResponseHandler( beginTxFuture ) );

            return beginTxFuture;
        }
    }



    @Override
    public CompletionStage<Bookmark> commitTransaction( Connection connection )
    {
        CompletableFuture<Bookmark> commitFuture = new CompletableFuture<>();

        ResponseHandler pullAllHandler = new CommitTxResponseHandler( commitFuture );
        connection.writeAndFlush(
                COMMIT_MESSAGE, NoOpResponseHandler.INSTANCE,
                PullAllMessage.PULL_ALL, pullAllHandler );

        return commitFuture;
    }

    @Override
    public CompletionStage<Void> rollbackTransaction( Connection connection )
    {
        CompletableFuture<Void> rollbackFuture = new CompletableFuture<>();

        ResponseHandler pullAllHandler = new RollbackTxResponseHandler( rollbackFuture );
        connection.writeAndFlush(
                ROLLBACK_MESSAGE, NoOpResponseHandler.INSTANCE,
                PullAllMessage.PULL_ALL, pullAllHandler );

        return rollbackFuture;
    }

    @Override
    public ResultCursorFactory runInAutoCommitTransaction(Connection connection, Query query, BookmarkHolder bookmarkHolder,
                                                          TransactionConfig config, boolean waitForRunResponse, long ignored )
    {
        // bookmarks are ignored for auto-commit transactions in this version of the protocol
        verifyBeforeTransaction( config, connection.databaseName() );
        return buildResultCursorFactory( connection, query, null, waitForRunResponse );
    }

    @Override
    public ResultCursorFactory runInUnmanagedTransaction(Connection connection, Query query, UnmanagedTransaction tx,
                                                         boolean waitForRunResponse, long ignored )
    {
        return buildResultCursorFactory( connection, query, tx, waitForRunResponse );
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    private static ResultCursorFactory buildResultCursorFactory(Connection connection, Query query,
                                                                UnmanagedTransaction tx, boolean waitForRunResponse )
    {
        String queryText = query.text();
        Map<String,Value> params = query.parameters().asMap( ofValue() );

        RunMessage runMessage = new RunMessage( queryText, params );
        RunResponseHandler runHandler = new RunResponseHandler( METADATA_EXTRACTOR );
        PullAllResponseHandler pullAllHandler = PullHandlers.newBoltV1PullAllHandler( query, runHandler, connection, tx );

        return new AsyncResultCursorOnlyFactory( connection, runMessage, runHandler, pullAllHandler, waitForRunResponse );
    }

    private void verifyBeforeTransaction( TransactionConfig config, DatabaseName databaseName )
    {
        if ( config != null && !config.isEmpty() )
        {
            throw txConfigNotSupported();
        }
        assertEmptyDatabaseName( databaseName, version() );
    }

    private static ClientException txConfigNotSupported()
    {
        return new ClientException( "Driver is connected to the database that does not support transaction configuration. " +
                "Please upgrade to neo4j 3.5.0 or later in order to use this functionality" );
    }

    static class SingleBookmarkHelper
    {
        private static final String BOOKMARK_PREFIX = "neo4j:bookmark:v1:tx";
        private static final long UNKNOWN_BOOKMARK_VALUE = -1;

        static Map<String,Value> asBeginTransactionParameters( Bookmark bookmark )
        {
            if ( bookmark.isEmpty() )
            {
                return emptyMap();
            }

            // Driver sends {bookmark: "max", bookmarks: ["one", "two", "max"]} instead of simple
            // {bookmarks: ["one", "two", "max"]} for backwards compatibility reasons. Old servers can only accept single
            // bookmark that is why driver has to parse and compare given list of bookmarks. This functionality will
            // eventually be removed.
            Map<String,Value> parameters = newHashMapWithSize( 1 );
            parameters.put( "bookmark", value( maxBookmark( bookmark.values() ) ) );
            parameters.put( "bookmarks", value( bookmark.values() ) );
            return parameters;
        }

        private static String maxBookmark( Iterable<String> bookmarks )
        {
            if ( bookmarks == null )
            {
                return null;
            }

            Iterator<String> iterator = bookmarks.iterator();

            if ( !iterator.hasNext() )
            {
                return null;
            }

            String maxBookmark = iterator.next();
            long maxValue = bookmarkValue( maxBookmark );

            while ( iterator.hasNext() )
            {
                String bookmark = iterator.next();
                long value = bookmarkValue( bookmark );

                if ( value > maxValue )
                {
                    maxBookmark = bookmark;
                    maxValue = value;
                }
            }

            return maxBookmark;
        }

        private static long bookmarkValue( String value )
        {
            if ( value != null && value.startsWith( BOOKMARK_PREFIX ) )
            {
                try
                {
                    return Long.parseLong( value.substring( BOOKMARK_PREFIX.length() ) );
                }
                catch ( NumberFormatException e )
                {
                    return UNKNOWN_BOOKMARK_VALUE;
                }
            }
            return UNKNOWN_BOOKMARK_VALUE;
        }
    }
}
