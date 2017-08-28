/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.internal.spi;

import java.util.Map;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.ServerInfo;

/**
 * A connection is an abstraction provided by an underlying transport implementation,
 * it is the medium that a session is conducted over.
 */
public interface Connection extends AutoCloseable
{
    /**
     * Initialize the connection. This must be done before any other action is allowed.
     * @param clientName should be the driver name and version: "java-driver/1.1.0"
     * @param authToken a map value
     */
    void init( String clientName, Map<String,Value> authToken );

    /**
     * Queue up a run action. The result handler will be called with metadata about the stream when that becomes
     * available for retrieval.
     * @param parameters a map value of parameters
     */
    void run( String statement, Map<String,Value> parameters, ResponseHandler handler );

    /**
     * Queue a discard all action, consuming any items left in the current stream.This will
     * close the stream once its completed, allowing another {@link #run(String, java.util.Map, ResponseHandler) run}
     */
    void discardAll( ResponseHandler handler );

    /**
     * Queue a pull-all action, output will be handed to the response handler once the pull starts. This will
     * close the stream once its completed, allowing another {@link #run(String, java.util.Map, ResponseHandler) run}
     */
    void pullAll( ResponseHandler handler );

    /**
     * Queue a reset action, throw {@link org.neo4j.driver.v1.exceptions.ClientException} if an ignored message is
     * received. This will close the stream once its completed, allowing another
     * {@link #run(String, java.util.Map, ResponseHandler) run}.
     */
    void reset();

    /**
     * Queue a ack_failure action, valid output could only be success. This will close the stream once it is completed,
     * allowing another {@link #run(String, java.util.Map, ResponseHandler) run}.
     */
    void ackFailure();

    /**
     * Ensure all outstanding actions are carried out on the server.
     */
    void sync();

    /**
     * Send all pending messages to the server and return the number of messages sent.
     */
    void flush();

    /**
     * Receive the next message available.
     */
    void receiveOne();

    @Override
    void close();

    /**
     * Test if the underlying socket connection with the server is still open.
     * When the socket connection with the server is closed,
     * the connection cannot take on any task, but be {@link #close() closed} to release resources it occupies.
     * Note: Invocation of {@link #close()} method would make this method to return false,
     * however this method cannot indicate whether {@link #close()} is already be called or not.
     * @return true if the socket connection with the server is open, otherwise false.
     */
    boolean isOpen();

    /**
     * Asynchronously sending reset to the socket output channel.
     */
    void resetAsync();

    /**
     * Return true if ack_failure message is temporarily muted as the failure message will be acked using reset instead
     * @return true if no ack_failre message should be sent when ackable failures are received.
     */
    boolean isAckFailureMuted();

    /**
     * Returns the basic information of the server connected to.
     * @return The basic information of the server connected to.
     */
    ServerInfo server();

    /**
     * Returns the BoltServerAddress connected to
     */
    BoltServerAddress boltServerAddress();
}
