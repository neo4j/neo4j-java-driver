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
package org.neo4j.driver.internal.spi;

import java.util.Map;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.Value;

public interface ResponseHandler
{
    void onSuccess( Map<String,Value> metadata );

    void onFailure( Throwable error );

    void onRecord( Value[] fields );

    /**
     * Tells whether this response handler is able to manage auto-read of the underlying connection using {@link Connection#enableAutoRead()} and
     * {@link Connection#disableAutoRead()}.
     * <p>
     * Implementations can use auto-read management to apply network-level backpressure when receiving a stream of records.
     * There should only be a single such handler active for a connection at one point in time. Otherwise, handlers can interfere and turn on/off auto-read
     * racing with each other. {@link InboundMessageDispatcher} is responsible for tracking these handlers and disabling auto-read management to maintain just
     * a single auto-read managing handler per connection.
     */
    default boolean canManageAutoRead()
    {
        return false;
    }

    /**
     * If this response handler is able to manage auto-read of the underlying connection, then this method signals it to
     * stop changing auto-read setting for the connection.
     */
    default void disableAutoReadManagement()
    {

    }
}
