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
package org.neo4j.driver.internal.messaging.v1;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.LegacyInternalStatementResultCursor;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.InitResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.AbstractPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.SessionPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.TransactionPullAllResponseHandler;
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
import org.neo4j.driver.react.result.InternalStatementResultCursor;
import org.neo4j.driver.react.result.RxStatementResultCursor;
import org.neo4j.driver.react.result.StatementResultCursorFactory;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.async.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.v1.Values.ofValue;

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
    public CompletionStage<Void> beginTransaction( Connection connection, Bookmarks bookmarks, TransactionConfig config )
    {
        if ( config != null && !config.isEmpty() )
        {
            return txConfigNotSupported();
        }

        if ( bookmarks.isEmpty() )
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
                    new RunMessage( BEGIN_QUERY, bookmarks.asBeginTransactionParameters() ), NoOpResponseHandler.INSTANCE,
                    PullAllMessage.PULL_ALL, new BeginTxResponseHandler( beginTxFuture ) );

            return beginTxFuture;
        }
    }

    @Override
    public CompletionStage<Bookmarks> commitTransaction( Connection connection )
    {
        CompletableFuture<Bookmarks> commitFuture = new CompletableFuture<>();

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
    public CompletionStage<StatementResultCursorFactory> runInAutoCommitTransaction( Connection connection, Statement statement,
            BookmarksHolder bookmarksHolder, TransactionConfig config, boolean waitForRunResponse )
    {
        // bookmarks are ignored for auto-commit transactions in this version of the protocol

        if ( config != null && !config.isEmpty() )
        {
            return txConfigNotSupported();
        }
        return runStatement( connection, statement, null, waitForRunResponse );
    }

    @Override
    public CompletionStage<StatementResultCursorFactory> runInExplicitTransaction( Connection connection, Statement statement, ExplicitTransaction tx,
            boolean waitForRunResponse )
    {
        return runStatement( connection, statement, tx, waitForRunResponse );
    }

    private static CompletionStage<StatementResultCursorFactory> runStatement( Connection connection, Statement statement,
            ExplicitTransaction tx, boolean waitForRunResponse )
    {
        String query = statement.text();
        Map<String,Value> params = statement.parameters().asMap( ofValue() );

        CompletableFuture<Void> runCompletedFuture = new CompletableFuture<>();
        RunResponseHandler runHandler = new RunResponseHandler( runCompletedFuture, METADATA_EXTRACTOR );
        AbstractPullAllResponseHandler pullAllHandler = newPullAllHandler( statement, runHandler, connection, tx );

        connection.writeAndFlush(
                new RunMessage( query, params ), runHandler,
                PullAllMessage.PULL_ALL, pullAllHandler );

        if ( waitForRunResponse )
        {
            // wait for response of RUN before proceeding
            return runCompletedFuture.thenApply( ignore ->
                    new AsyncResultCursorOnlyFactory( runHandler, pullAllHandler ) );
        }
        else
        {
            return completedFuture( new AsyncResultCursorOnlyFactory( runHandler, pullAllHandler ) );
        }
    }

    private static AbstractPullAllResponseHandler newPullAllHandler( Statement statement, RunResponseHandler runHandler,
            Connection connection, ExplicitTransaction tx )
    {
        if ( tx != null )
        {
            return new TransactionPullAllResponseHandler( statement, runHandler, connection, tx, METADATA_EXTRACTOR );
        }
        return new SessionPullAllResponseHandler( statement, runHandler, connection, BookmarksHolder.NO_OP, METADATA_EXTRACTOR );
    }

    private static <T> CompletionStage<T> txConfigNotSupported()
    {
        return Futures.failedFuture( new ClientException( "Driver is connected to the database that does not support transaction configuration. " +
                                                          "Please upgrade to neo4j 3.5.0 or later in order to use this functionality" ) );
    }

    public static class AsyncResultCursorOnlyFactory implements StatementResultCursorFactory
    {
        private final RunResponseHandler runHandler;
        private final AbstractPullAllResponseHandler pullAllHandler;

        public AsyncResultCursorOnlyFactory( RunResponseHandler runHandler, AbstractPullAllResponseHandler pullAllHandler )
        {
            this.runHandler = runHandler;
            this.pullAllHandler = pullAllHandler;
        }

        @Override
        public InternalStatementResultCursor asyncResult()
        {
            return new LegacyInternalStatementResultCursor( runHandler, pullAllHandler );
        }

        @Override
        public RxStatementResultCursor rxResult()
        {
            throw new ClientException( "Earlier Bolt protocol does not support react API. In order to use the react API, make sure you have 4.0+ servers." );
        }
    }
}
