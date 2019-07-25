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
package org.neo4j.driver.internal.messaging.v3;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Statement;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.async.ExplicitTransaction;
import org.neo4j.driver.internal.cursor.AsyncResultCursorOnlyFactory;
import org.neo4j.driver.internal.cursor.StatementResultCursorFactory;
import org.neo4j.driver.internal.handlers.AbstractPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.HelloResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.GoodbyeMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.MetadataExtractor;

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.handlers.PullHandlers.newBoltV3PullAllHandler;
import static org.neo4j.driver.internal.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.assertEmptyDatabaseName;
import static org.neo4j.driver.internal.messaging.request.RollbackMessage.ROLLBACK;
import static org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage.autoCommitTxRunMessage;
import static org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage.explicitTxRunMessage;

public class BoltProtocolV3 implements BoltProtocol
{
    public static final int VERSION = 3;

    public static final BoltProtocol INSTANCE = new BoltProtocolV3();

    public static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor( "t_first", "t_last" );

    @Override
    public MessageFormat createMessageFormat()
    {
        return new MessageFormatV3();
    }

    @Override
    public void initializeChannel( String userAgent, Map<String,Value> authToken, ChannelPromise channelInitializedPromise )
    {
        Channel channel = channelInitializedPromise.channel();

        HelloMessage message = new HelloMessage( userAgent, authToken );
        HelloResponseHandler handler = new HelloResponseHandler( channelInitializedPromise );

        messageDispatcher( channel ).enqueue( handler );
        channel.writeAndFlush( message, channel.voidPromise() );
    }

    @Override
    public void prepareToCloseChannel( Channel channel )
    {
        GoodbyeMessage message = GoodbyeMessage.GOODBYE;
        messageDispatcher( channel ).enqueue( NoOpResponseHandler.INSTANCE );
        channel.writeAndFlush( message, channel.voidPromise() );
    }

    @Override
    public CompletionStage<Void> beginTransaction( Connection connection, InternalBookmark bookmark, TransactionConfig config )
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
    public CompletionStage<InternalBookmark> commitTransaction( Connection connection )
    {
        CompletableFuture<InternalBookmark> commitFuture = new CompletableFuture<>();
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
    public StatementResultCursorFactory runInAutoCommitTransaction( Connection connection, Statement statement,
            BookmarkHolder bookmarkHolder, TransactionConfig config, boolean waitForRunResponse )
    {
        verifyDatabaseNameBeforeTransaction( connection.databaseName() );
        RunWithMetadataMessage runMessage =
                autoCommitTxRunMessage( statement, config, connection.databaseName(), connection.mode(), bookmarkHolder.getBookmark() );
        return buildResultCursorFactory( connection, statement, bookmarkHolder, null, runMessage, waitForRunResponse );
    }

    @Override
    public StatementResultCursorFactory runInExplicitTransaction( Connection connection, Statement statement, ExplicitTransaction tx,
            boolean waitForRunResponse )
    {
        RunWithMetadataMessage runMessage = explicitTxRunMessage( statement );
        return buildResultCursorFactory( connection, statement, BookmarkHolder.NO_OP, tx, runMessage, waitForRunResponse );
    }

    protected StatementResultCursorFactory buildResultCursorFactory( Connection connection, Statement statement, BookmarkHolder bookmarkHolder,
            ExplicitTransaction tx, RunWithMetadataMessage runMessage, boolean waitForRunResponse )
    {
        RunResponseHandler runHandler = new RunResponseHandler( METADATA_EXTRACTOR );
        AbstractPullAllResponseHandler pullHandler = newBoltV3PullAllHandler( statement, runHandler, connection, bookmarkHolder, tx );

        return new AsyncResultCursorOnlyFactory( connection, runMessage, runHandler, pullHandler, waitForRunResponse );
    }

    protected void verifyDatabaseNameBeforeTransaction( String databaseName )
    {
        assertEmptyDatabaseName( databaseName, version() );
    }

    @Override
    public int version()
    {
        return VERSION;
    }
}
