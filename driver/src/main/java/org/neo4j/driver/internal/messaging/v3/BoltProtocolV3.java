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
package org.neo4j.driver.internal.messaging.v3;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cursor.AsyncResultCursorOnlyFactory;
import org.neo4j.driver.internal.cursor.ResultCursorFactory;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.HelloResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.GoodbyeMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.MetadataExtractor;

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.handlers.PullHandlers.newBoltV3PullAllHandler;
import static org.neo4j.driver.internal.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.assertEmptyDatabaseName;
import static org.neo4j.driver.internal.messaging.request.RollbackMessage.ROLLBACK;
import static org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage.autoCommitTxRunMessage;
import static org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage.unmanagedTxRunMessage;

public class BoltProtocolV3 implements BoltProtocol
{
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion( 3, 0 );

    public static final BoltProtocol INSTANCE = new BoltProtocolV3();

    public static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor( "t_first", "t_last" );

    @Override
    public MessageFormat createMessageFormat()
    {
        return new MessageFormatV3();
    }

    @Override
    public void initializeChannel( String userAgent, AuthToken authToken, RoutingContext routingContext, ChannelPromise channelInitializedPromise )
    {
        Channel channel = channelInitializedPromise.channel();
        HelloMessage message;

        if ( routingContext.isServerRoutingEnabled() )
        {
            message = new HelloMessage( userAgent, ( ( InternalAuthToken ) authToken ).toMap(), routingContext.toMap() );
        }
        else
        {
            message = new HelloMessage( userAgent, ( ( InternalAuthToken ) authToken ).toMap(), null );
        }

        HelloResponseHandler handler = new HelloResponseHandler( channelInitializedPromise, version() );

        messageDispatcher( channel ).enqueue( handler );
        channel.writeAndFlush( message, channel.voidPromise() );
    }

    @Override
    public void prepareToCloseChannel( Channel channel )
    {
        InboundMessageDispatcher messageDispatcher = messageDispatcher( channel );

        GoodbyeMessage message = GoodbyeMessage.GOODBYE;
        messageDispatcher.enqueue( NoOpResponseHandler.INSTANCE );
        channel.writeAndFlush( message, channel.voidPromise() );

        messageDispatcher.prepareToCloseChannel( );
    }

    @Override
    public CompletionStage<Void> beginTransaction( Connection connection, Bookmark bookmark, TransactionConfig config )
    {
        try
        {
            verifyDatabaseNameBeforeTransaction( connection.databaseName() );
        }
        catch ( Exception error )
        {
            return Futures.failedFuture( error );
        }

        BeginMessage beginMessage = new BeginMessage( bookmark, config, connection.databaseName(), connection.mode() );

        if ( bookmark.isEmpty() )
        {
            connection.write( beginMessage, NoOpResponseHandler.INSTANCE );
            return Futures.completedWithNull();
        }
        else
        {
            CompletableFuture<Void> beginTxFuture = new CompletableFuture<>();
            connection.writeAndFlush( beginMessage, new BeginTxResponseHandler( beginTxFuture ) );
            return beginTxFuture;
        }
    }

    @Override
    public CompletionStage<Bookmark> commitTransaction( Connection connection )
    {
        CompletableFuture<Bookmark> commitFuture = new CompletableFuture<>();
        connection.writeAndFlush( COMMIT, new CommitTxResponseHandler( commitFuture ) );
        return commitFuture;
    }

    @Override
    public CompletionStage<Void> rollbackTransaction( Connection connection )
    {
        CompletableFuture<Void> rollbackFuture = new CompletableFuture<>();
        connection.writeAndFlush( ROLLBACK, new RollbackTxResponseHandler( rollbackFuture ) );
        return rollbackFuture;
    }

    @Override
    public ResultCursorFactory runInAutoCommitTransaction(Connection connection, Query query, BookmarkHolder bookmarkHolder,
                                                          TransactionConfig config, boolean waitForRunResponse, long fetchSize )
    {
        verifyDatabaseNameBeforeTransaction( connection.databaseName() );
        RunWithMetadataMessage runMessage =
                autoCommitTxRunMessage(query, config, connection.databaseName(), connection.mode(), bookmarkHolder.getBookmark() );
        return buildResultCursorFactory( connection, query, bookmarkHolder, null, runMessage, waitForRunResponse, fetchSize );
    }

    @Override
    public ResultCursorFactory runInUnmanagedTransaction(Connection connection, Query query, UnmanagedTransaction tx,
                                                         boolean waitForRunResponse, long fetchSize )
    {
        RunWithMetadataMessage runMessage = unmanagedTxRunMessage(query);
        return buildResultCursorFactory( connection, query, BookmarkHolder.NO_OP, tx, runMessage, waitForRunResponse, fetchSize );
    }

    protected ResultCursorFactory buildResultCursorFactory(Connection connection, Query query, BookmarkHolder bookmarkHolder,
                                                           UnmanagedTransaction tx, RunWithMetadataMessage runMessage, boolean waitForRunResponse, long ignored )
    {
        RunResponseHandler runHandler = new RunResponseHandler( METADATA_EXTRACTOR );
        PullAllResponseHandler pullHandler = newBoltV3PullAllHandler(query, runHandler, connection, bookmarkHolder, tx );

        return new AsyncResultCursorOnlyFactory( connection, runMessage, runHandler, pullHandler, waitForRunResponse );
    }

    protected void verifyDatabaseNameBeforeTransaction( DatabaseName databaseName )
    {
        assertEmptyDatabaseName( databaseName, version() );
    }

    @Override
    public BoltProtocolVersion version()
    {
        return VERSION;
    }
}
