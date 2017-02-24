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

import org.neo4j.driver.internal.SessionResourcesHandler;
import org.neo4j.driver.internal.util.Clock;

public interface PooledConnection extends Connection
{
    /**
     * If there are any errors that occur on this connection, notify the given handler
     * about them. This is used in the driver to clean up resources associated with
     * the connection, like an open transaction.
     *
     * @param resourcesHandler To be notified on error.
     */
    void setResourcesHandler( SessionResourcesHandler resourcesHandler );

    /**
     * Check if this connection experienced any unrecoverable errors. Connections with unrecoverable errors should be
     * closed and not returned to the pool for future reuse.
     *
     * @return {@code true} if any unrecoverable error happened, {@code false} otherwise.
     */
    boolean hasUnrecoverableErrors();

    /**
     * Timestamp of when this connection was used. This timestamp is updated when connection is returned to the pool.
     *
     * @return timestamp as returned by {@link Clock#millis()}.
     */
    long lastUsedTimestamp();

    /**
     * Destroy this connection and associated network resources.
     */
    void dispose();
}
