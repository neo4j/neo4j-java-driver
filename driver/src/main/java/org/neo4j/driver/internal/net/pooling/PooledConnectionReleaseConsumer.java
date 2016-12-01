/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.internal.net.pooling;

import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.util.Function;

/**
 * The responsibility of the PooledConnectionReleaseConsumer is to release valid connections
 * back to the connections queue.
 */
class PooledConnectionReleaseConsumer implements Consumer<PooledConnection>
{
    private final BlockingPooledConnectionQueue connections;
    private final Function<PooledConnection, Boolean> validConnection;

    PooledConnectionReleaseConsumer( BlockingPooledConnectionQueue connections,
            Function<PooledConnection, Boolean> validConnection)
    {
        this.connections = connections;
        this.validConnection = validConnection;
    }

    @Override
    public void accept( PooledConnection pooledConnection )
    {
        if ( validConnection.apply( pooledConnection ) )
        {
            connections.offer( pooledConnection );
        }
        else
        {
            pooledConnection.dispose();
        }
    }
}
