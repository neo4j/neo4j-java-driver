/**
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
    private final AtomicBoolean driverStopped;
    private final Function<PooledConnection, Boolean> validConnection;

    PooledConnectionReleaseConsumer( BlockingPooledConnectionQueue connections, AtomicBoolean driverStopped,
            Function<PooledConnection, Boolean> validConnection)
    {
        this.connections = connections;
        this.driverStopped = driverStopped;
        this.validConnection = validConnection;
    }

    @Override
    public void accept( PooledConnection pooledConnection )
    {
        if( driverStopped.get() )
        {
            // if the driver already closed, then no need to try to return to pool, just directly close this connection
            pooledConnection.dispose();
        }
        else if ( validConnection.apply( pooledConnection ) )
        {
            boolean released = connections.offer( pooledConnection );
            if( !released )
            {
                // if the connection could be put back to the pool, then we let the pool to manage it.
                // Otherwise, we close the connection directly here.
                pooledConnection.dispose();
            }
            else if ( driverStopped.get() )
            {
                // If our adding the pooledConnection to the queue was racing with the closing of the driver,
                // then the loop where the driver is closing all available connections might not observe our newly
                // added connection. Thus, we must attempt to remove a connection and dispose it. It doesn't matter
                // which connection we get back, because other threads might be in the same situation as ours. It only
                // matters that we added *a* connection that might not be observed by the loop, and that we dispose of
                // *a* connection in response.
                connections.terminate();
            }
        }
        else
        {
            pooledConnection.dispose();
        }
    }
}
