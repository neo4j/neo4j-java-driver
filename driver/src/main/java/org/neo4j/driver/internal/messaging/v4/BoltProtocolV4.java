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
package org.neo4j.driver.internal.messaging.v4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.HelloResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.GoodbyeMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.messaging.v3.MessageFormatV3;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.react.result.InternalStatementResultCursorFactory;
import org.neo4j.driver.react.result.StatementResultCursorFactory;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.Value;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.async.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.driver.internal.messaging.request.RollbackMessage.ROLLBACK;
import static org.neo4j.driver.v1.Values.ofValue;

public class BoltProtocolV4 implements BoltProtocol
{
    public static final int VERSION = 4;

    public static final BoltProtocol INSTANCE = new BoltProtocolV4();

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
    public CompletionStage<Void> beginTransaction( Connection connection, Bookmarks bookmarks, TransactionConfig config )
    {
        BeginMessage beginMessage = new BeginMessage( bookmarks, config );

        if ( bookmarks.isEmpty() )
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
    public CompletionStage<Bookmarks> commitTransaction( Connection connection )
    {
        CompletableFuture<Bookmarks> commitFuture = new CompletableFuture<>();
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
    public CompletionStage<StatementResultCursorFactory> runInAutoCommitTransaction( Connection connection, Statement statement,
            BookmarksHolder bookmarksHolder, TransactionConfig config, boolean waitForRunResponse )
    {
        return runStatement( connection, statement, bookmarksHolder, null, config, waitForRunResponse );
    }

    @Override
    public CompletionStage<StatementResultCursorFactory> runInExplicitTransaction( Connection connection, Statement statement, ExplicitTransaction tx,
            boolean waitForRunResponse )
    {
        return runStatement( connection, statement, BookmarksHolder.NO_OP, tx, TransactionConfig.empty(), waitForRunResponse );
    }

    private static CompletionStage<StatementResultCursorFactory> runStatement( Connection connection, Statement statement, BookmarksHolder bookmarksHolder,
            ExplicitTransaction tx, TransactionConfig config, boolean waitForRunResponse )
    {
        String query = statement.text();
        Map<String,Value> params = statement.parameters().asMap( ofValue() );

        CompletableFuture<Void> runCompletedFuture = new CompletableFuture<>();
        Message runMessage = new RunWithMetadataMessage( query, params, bookmarksHolder.getBookmarks(), config );
        RunResponseHandler runHandler = new RunResponseHandler( runCompletedFuture, METADATA_EXTRACTOR );
        connection.writeAndFlush( runMessage, runHandler );

        if ( waitForRunResponse )
        {
            // wait for response of RUN before proceeding
            return runCompletedFuture.thenApply( ignore ->
                    new InternalStatementResultCursorFactory( runHandler, statement, connection, bookmarksHolder, tx ) );
        }
        else
        {
            return completedFuture( new InternalStatementResultCursorFactory( runHandler, statement, connection, bookmarksHolder, tx ) );
        }
    }
}
