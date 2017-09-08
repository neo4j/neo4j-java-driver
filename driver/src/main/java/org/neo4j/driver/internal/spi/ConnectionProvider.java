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

import org.neo4j.driver.internal.async.AsyncConnection;
import org.neo4j.driver.internal.async.EventLoopAwareFuture;
import org.neo4j.driver.v1.AccessMode;

/**
 * Interface defines a layer used by the driver to obtain connections. It is meant to be the only component that
 * differs between "direct" and "routing" driver.
 */
public interface ConnectionProvider extends AutoCloseable
{
    /**
     * Acquire new {@link PooledConnection pooled connection} for the given {@link AccessMode mode}.
     *
     * @param mode the access mode for the connection.
     * @return free or new pooled connection.
     */
    PooledConnection acquireConnection( AccessMode mode );

    EventLoopAwareFuture<AsyncConnection> acquireAsyncConnection( AccessMode mode );
}
