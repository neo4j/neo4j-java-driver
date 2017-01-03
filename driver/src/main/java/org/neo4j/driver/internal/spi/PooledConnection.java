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

import org.neo4j.driver.v1.Value;

public interface PooledConnection extends Connection
{
    /**
     * If there are any errors that occur on this connection, invoke the given runnable.
     * This is used in the driver to clean up resources associated with the connection, like an open transaction.
     *
     * @param runnable To be run on error.
     */
    void onError( Runnable runnable );

    /**
     * Return true if there is any unrecoverable error left in this connection in previous use.
     * If the connection has any unrecoverable errors, then it cannot be pooled but disposed directly.
     * @return true if there is any unrecoverable error left in this connection in previous use.
     */
    boolean hasUnrecoverableErrors();

    /**
     * Dispose all resources hold by the real connection.
     */
    void dispose();

    /**
     * Return the last used timestamp for pooling
     * @return the last used timestamp
     */
    long lastUsedTimestamp();

    // Override the methods to not throw any exceptions
    @Override
    void init( String clientName, Map<String,Value> authToken );

    @Override
    void run( String statement, Map<String,Value> parameters, Collector collector );

    @Override
    void discardAll( Collector collector );

    @Override
    void pullAll( Collector collector );

    @Override
    void reset();

    @Override
    void ackFailure();

    @Override
    void sync();

    @Override
    void flush();

    @Override
    void receiveOne();

    @Override
    void resetAsync();
}
