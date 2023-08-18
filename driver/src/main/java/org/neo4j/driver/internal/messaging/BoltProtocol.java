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
package org.neo4j.driver.internal.messaging;

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.protocolVersion;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import java.time.Clock;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.BoltAgent;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cursor.ResultCursorFactory;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.messaging.v41.BoltProtocolV41;
import org.neo4j.driver.internal.messaging.v42.BoltProtocolV42;
import org.neo4j.driver.internal.messaging.v43.BoltProtocolV43;
import org.neo4j.driver.internal.messaging.v44.BoltProtocolV44;
import org.neo4j.driver.internal.messaging.v5.BoltProtocolV5;
import org.neo4j.driver.internal.messaging.v51.BoltProtocolV51;
import org.neo4j.driver.internal.messaging.v52.BoltProtocolV52;
import org.neo4j.driver.internal.messaging.v53.BoltProtocolV53;
import org.neo4j.driver.internal.spi.Connection;

public interface BoltProtocol {
    /**
     * Instantiate {@link MessageFormat} used by this Bolt protocol verison.
     *
     * @return new message format.
     */
    MessageFormat createMessageFormat();

    /**
     * Initialize channel after it is connected and handshake selected this protocol version.
     *
     * @param userAgent                 the user agent string.
     * @param boltAgent                 the bolt agent
     * @param authToken                 the authentication token.
     * @param routingContext            the configured routing context
     * @param channelInitializedPromise the promise to be notified when initialization is completed.
     * @param notificationConfig        the notification configuration
     * @param clock                     the clock to use
     */
    void initializeChannel(
            String userAgent,
            BoltAgent boltAgent,
            AuthToken authToken,
            RoutingContext routingContext,
            ChannelPromise channelInitializedPromise,
            NotificationConfig notificationConfig,
            Clock clock);

    /**
     * Prepare to close channel before it is closed.
     * @param channel the channel to close.
     */
    void prepareToCloseChannel(Channel channel);

    /**
     * Begin an unmanaged transaction.
     *
     * @param connection         the connection to use.
     * @param bookmarks          the bookmarks. Never null, should be empty when there are no bookmarks.
     * @param config             the transaction configuration. Never null, should be {@link TransactionConfig#empty()} when absent.
     * @param txType             the Kernel transaction type
     * @param notificationConfig the notification configuration
     * @param logging            the driver logging
     * @param flush              defines whether to flush the message to the connection
     * @return a completion stage completed when transaction is started or completed exceptionally when there was a failure.
     */
    CompletionStage<Void> beginTransaction(
            Connection connection,
            Set<Bookmark> bookmarks,
            TransactionConfig config,
            String txType,
            NotificationConfig notificationConfig,
            Logging logging,
            boolean flush);

    /**
     * Commit the unmanaged transaction.
     *
     * @param connection the connection to use.
     * @return a completion stage completed with a bookmark when transaction is committed or completed exceptionally when there was a failure.
     */
    CompletionStage<DatabaseBookmark> commitTransaction(Connection connection);

    /**
     * Rollback the unmanaged transaction.
     *
     * @param connection the connection to use.
     * @return a completion stage completed when transaction is rolled back or completed exceptionally when there was a failure.
     */
    CompletionStage<Void> rollbackTransaction(Connection connection);

    /**
     * Execute the given query in an auto-commit transaction, i.e. {@link Session#run(Query)}.
     *
     * @param connection         the network connection to use.
     * @param query              the cypher to execute.
     * @param bookmarkConsumer   the database bookmark consumer.
     * @param config             the transaction config for the implicitly started auto-commit transaction.
     * @param fetchSize          the record fetch size for PULL message.
     * @param notificationConfig the notification configuration
     * @param logging            the driver logging
     * @return stage with cursor.
     */
    ResultCursorFactory runInAutoCommitTransaction(
            Connection connection,
            Query query,
            Set<Bookmark> bookmarks,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            TransactionConfig config,
            long fetchSize,
            NotificationConfig notificationConfig,
            Logging logging);

    /**
     * Execute the given query in a running unmanaged transaction, i.e. {@link Transaction#run(Query)}.
     *
     * @param connection the network connection to use.
     * @param query      the cypher to execute.
     * @param tx         the transaction which executes the query.
     * @param fetchSize  the record fetch size for PULL message.
     * @return stage with cursor.
     */
    ResultCursorFactory runInUnmanagedTransaction(
            Connection connection, Query query, UnmanagedTransaction tx, long fetchSize);

    /**
     * Returns the protocol version. It can be used for version specific error messages.
     * @return the protocol version.
     */
    BoltProtocolVersion version();

    /**
     * Obtain an instance of the protocol for the given channel.
     *
     * @param channel the channel to get protocol for.
     * @return the protocol.
     * @throws ClientException when unable to find protocol version for the given channel.
     */
    static BoltProtocol forChannel(Channel channel) {
        return forVersion(protocolVersion(channel));
    }

    /**
     * Obtain an instance of the protocol for the given channel.
     *
     * @param version the version of the protocol.
     * @return the protocol.
     * @throws ClientException when unable to find protocol with the given version.
     */
    static BoltProtocol forVersion(BoltProtocolVersion version) {
        if (BoltProtocolV3.VERSION.equals(version)) {
            return BoltProtocolV3.INSTANCE;
        } else if (BoltProtocolV4.VERSION.equals(version)) {
            return BoltProtocolV4.INSTANCE;
        } else if (BoltProtocolV41.VERSION.equals(version)) {
            return BoltProtocolV41.INSTANCE;
        } else if (BoltProtocolV42.VERSION.equals(version)) {
            return BoltProtocolV42.INSTANCE;
        } else if (BoltProtocolV43.VERSION.equals(version)) {
            return BoltProtocolV43.INSTANCE;
        } else if (BoltProtocolV44.VERSION.equals(version)) {
            return BoltProtocolV44.INSTANCE;
        } else if (BoltProtocolV5.VERSION.equals(version)) {
            return BoltProtocolV5.INSTANCE;
        } else if (BoltProtocolV51.VERSION.equals(version)) {
            return BoltProtocolV51.INSTANCE;
        } else if (BoltProtocolV52.VERSION.equals(version)) {
            return BoltProtocolV52.INSTANCE;
        } else if (BoltProtocolV53.VERSION.equals(version)) {
            return BoltProtocolV53.INSTANCE;
        }
        throw new ClientException("Unknown protocol version: " + version);
    }
}
