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
package org.neo4j.driver.internal.pool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.spi.StreamCollector;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Value;

/**
 * The responsibility of the PooledConnectionReleaseConsumer is to release valid connections
 * back to the connections queue.
 */
class PooledConnectionReleaseConsumer implements Consumer<PooledConnection>
{
    private final BlockingQueue<PooledConnection> connections;
    private final long minIdleBeforeConnectionTest;
    private static final Map<String,Value> NO_PARAMETERS = new HashMap<>();
    private final AtomicBoolean driverStopped;

    PooledConnectionReleaseConsumer( BlockingQueue<PooledConnection> connections, AtomicBoolean driverStopped,
            Config config )
    {
        this.connections = connections;
        this.driverStopped = driverStopped;
        this.minIdleBeforeConnectionTest = config.idleTimeBeforeConnectionTest();
    }

    @Override
    public void accept( PooledConnection pooledConnection )
    {
        if( driverStopped.get() )
        {
            // if the driver already closed, then no need to try to return to pool, just directly close this connection
            pooledConnection.dispose();
        }
        else if ( validConnection( pooledConnection ) )
        {
            boolean released = connections.offer( pooledConnection );
            if( !released )
            {
                // if the connection could be put back to the pool, then we let the pool to manage it.
                // Otherwise, we close the connection directly here.
                pooledConnection.dispose();
            }
        }
    }

    boolean validConnection( PooledConnection pooledConnection )
    {
        return !pooledConnection.hasUnrecoverableErrors() &&
               (pooledConnection.idleTime() <= minIdleBeforeConnectionTest || ping( pooledConnection ));
    }

    private boolean ping( PooledConnection conn )
    {
        try
        {
            conn.run( "RETURN 1 // JavaDriver poll to test connection", NO_PARAMETERS, StreamCollector.NO_OP );
            conn.pullAll( StreamCollector.NO_OP );
            conn.sync();
            return true;
        }
        catch ( Throwable e )
        {
            return false;
        }
    }
}
