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

import org.neo4j.driver.Statement;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.async.ExplicitTransaction;
import org.neo4j.driver.internal.cursor.AsyncResultCursorOnlyFactory;
import org.neo4j.driver.internal.cursor.StatementResultCursorFactory;
import org.neo4j.driver.internal.handlers.AbstractPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.InitResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
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

import static org.neo4j.driver.Values.ofValue;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.assertEmptyDatabaseName;

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
        try
        {
            verifyBeforeTransaction( config, connection.databaseName() );
        }
        catch ( Exception error )
        {
            return Futures.failedFuture( error );
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
    public StatementResultCursorFactory runInAutoCommitTransaction( Connection connection, Statement statement,
            BookmarksHolder bookmarksHolder, TransactionConfig config, boolean waitForRunResponse )
    {
        // bookmarks are ignored for auto-commit transactions in this version of the protocol
        verifyBeforeTransaction( config, connection.databaseName() );
        return buildResultCursorFactory( connection, statement, null, waitForRunResponse );
    }

    @Override
    public StatementResultCursorFactory runInExplicitTransaction( Connection connection, Statement statement, ExplicitTransaction tx,
            boolean waitForRunResponse )
    {
        return buildResultCursorFactory( connection, statement, tx, waitForRunResponse );
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    private static StatementResultCursorFactory buildResultCursorFactory( Connection connection, Statement statement,
            ExplicitTransaction tx, boolean waitForRunResponse )
    {
        String query = statement.text();
        Map<String,Value> params = statement.parameters().asMap( ofValue() );

        RunMessage runMessage = new RunMessage( query, params );
        RunResponseHandler runHandler = new RunResponseHandler( METADATA_EXTRACTOR );
        AbstractPullAllResponseHandler pullAllHandler = PullHandlers.newBoltV1PullAllHandler( statement, runHandler, connection, tx );

        return new AsyncResultCursorOnlyFactory( connection, runMessage, runHandler, pullAllHandler, waitForRunResponse );
    }

    private void verifyBeforeTransaction( TransactionConfig config, String databaseName )
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
}
