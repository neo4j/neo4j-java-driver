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

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.handlers.PullHandlers.newBoltV3PullAllHandler;
import static org.neo4j.driver.internal.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.assertEmptyDatabaseName;
import static org.neo4j.driver.internal.messaging.request.RollbackMessage.ROLLBACK;
import static org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage.autoCommitTxRunMessage;
import static org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage.unmanagedTxRunMessage;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import java.time.Clock;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.internal.BoltAgent;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cursor.AsyncResultCursorOnlyFactory;
import org.neo4j.driver.internal.cursor.ResultCursorFactory;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.HelloResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
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

public class BoltProtocolV3 implements BoltProtocol {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(3, 0);

    public static final BoltProtocol INSTANCE = new BoltProtocolV3();

    public static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor("t_first", "t_last");

    @Override
    public MessageFormat createMessageFormat() {
        return new MessageFormatV3();
    }

    @Override
    public void initializeChannel(
            String userAgent,
            BoltAgent boltAgent,
            AuthToken authToken,
            RoutingContext routingContext,
            ChannelPromise channelInitializedPromise,
            NotificationConfig notificationConfig,
            Clock clock) {
        var exception = verifyNotificationConfigSupported(notificationConfig);
        if (exception != null) {
            channelInitializedPromise.setFailure(exception);
            return;
        }
        var channel = channelInitializedPromise.channel();
        HelloMessage message;

        if (routingContext.isServerRoutingEnabled()) {
            message = new HelloMessage(
                    userAgent,
                    null,
                    ((InternalAuthToken) authToken).toMap(),
                    routingContext.toMap(),
                    includeDateTimeUtcPatchInHello(),
                    notificationConfig);
        } else {
            message = new HelloMessage(
                    userAgent,
                    null,
                    ((InternalAuthToken) authToken).toMap(),
                    null,
                    includeDateTimeUtcPatchInHello(),
                    notificationConfig);
        }

        var handler = new HelloResponseHandler(channelInitializedPromise, clock);

        messageDispatcher(channel).enqueue(handler);
        channel.writeAndFlush(message, channel.voidPromise());
    }

    @Override
    public void prepareToCloseChannel(Channel channel) {
        var messageDispatcher = messageDispatcher(channel);

        var message = GoodbyeMessage.GOODBYE;
        messageDispatcher.enqueue(NoOpResponseHandler.INSTANCE);
        channel.writeAndFlush(message, channel.voidPromise());

        messageDispatcher.prepareToCloseChannel();
    }

    @Override
    public CompletionStage<Void> beginTransaction(
            Connection connection,
            Set<Bookmark> bookmarks,
            TransactionConfig config,
            String txType,
            NotificationConfig notificationConfig,
            Logging logging,
            boolean flush) {
        var exception = verifyNotificationConfigSupported(notificationConfig);
        if (exception != null) {
            return CompletableFuture.failedStage(exception);
        }
        try {
            verifyDatabaseNameBeforeTransaction(connection.databaseName());
        } catch (Exception error) {
            return Futures.failedFuture(error);
        }

        var beginTxFuture = new CompletableFuture<Void>();
        var beginMessage = new BeginMessage(
                bookmarks,
                config,
                connection.databaseName(),
                connection.mode(),
                connection.impersonatedUser(),
                txType,
                notificationConfig,
                logging);
        var handler = new BeginTxResponseHandler(beginTxFuture);
        if (flush) {
            connection.writeAndFlush(beginMessage, handler);
        } else {
            connection.write(beginMessage, handler);
        }
        return beginTxFuture;
    }

    @Override
    public CompletionStage<DatabaseBookmark> commitTransaction(Connection connection) {
        var commitFuture = new CompletableFuture<DatabaseBookmark>();
        connection.writeAndFlush(COMMIT, new CommitTxResponseHandler(commitFuture));
        return commitFuture;
    }

    @Override
    public CompletionStage<Void> rollbackTransaction(Connection connection) {
        var rollbackFuture = new CompletableFuture<Void>();
        connection.writeAndFlush(ROLLBACK, new RollbackTxResponseHandler(rollbackFuture));
        return rollbackFuture;
    }

    @Override
    public ResultCursorFactory runInAutoCommitTransaction(
            Connection connection,
            Query query,
            Set<Bookmark> bookmarks,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            TransactionConfig config,
            long fetchSize,
            NotificationConfig notificationConfig,
            Logging logging) {
        var exception = verifyNotificationConfigSupported(notificationConfig);
        if (exception != null) {
            throw exception;
        }
        verifyDatabaseNameBeforeTransaction(connection.databaseName());
        var runMessage = autoCommitTxRunMessage(
                query,
                config,
                connection.databaseName(),
                connection.mode(),
                bookmarks,
                connection.impersonatedUser(),
                notificationConfig,
                logging);
        return buildResultCursorFactory(connection, query, bookmarkConsumer, null, runMessage, fetchSize);
    }

    @Override
    public ResultCursorFactory runInUnmanagedTransaction(
            Connection connection, Query query, UnmanagedTransaction tx, long fetchSize) {
        var runMessage = unmanagedTxRunMessage(query);
        return buildResultCursorFactory(connection, query, (ignored) -> {}, tx, runMessage, fetchSize);
    }

    protected ResultCursorFactory buildResultCursorFactory(
            Connection connection,
            Query query,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            UnmanagedTransaction tx,
            RunWithMetadataMessage runMessage,
            long ignored) {
        var runFuture = new CompletableFuture<Void>();
        var runHandler = new RunResponseHandler(runFuture, METADATA_EXTRACTOR, connection, tx);
        var pullHandler = newBoltV3PullAllHandler(query, runHandler, connection, bookmarkConsumer, tx);

        return new AsyncResultCursorOnlyFactory(connection, runMessage, runHandler, runFuture, pullHandler);
    }

    protected void verifyDatabaseNameBeforeTransaction(DatabaseName databaseName) {
        assertEmptyDatabaseName(databaseName, version());
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    protected boolean includeDateTimeUtcPatchInHello() {
        return false;
    }

    protected Neo4jException verifyNotificationConfigSupported(NotificationConfig notificationConfig) {
        Neo4jException exception = null;
        if (notificationConfig != null && !notificationConfig.equals(NotificationConfig.defaultConfig())) {
            exception = new UnsupportedFeatureException(String.format(
                    "Notification configuration is not supported on Bolt %s",
                    version().toString()));
        }
        return exception;
    }
}
