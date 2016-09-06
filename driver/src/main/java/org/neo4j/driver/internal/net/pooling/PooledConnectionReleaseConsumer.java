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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.util.Consumer;
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
            PoolSettings poolSettings)
    {
        this.connections = connections;
        this.driverStopped = driverStopped;
        this.minIdleBeforeConnectionTest = poolSettings.idleTimeBeforeConnectionTest();
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
            else if ( driverStopped.get() )
            {
                // If our adding the pooledConnection to the queue was racing with the closing of the driver,
                // then the loop where the driver is closing all available connections might not observe our newly
                // added connection. Thus, we must attempt to remove a connection and dispose it. It doesn't matter
                // which connection we get back, because other threads might be in the same situation as ours. It only
                // matters that we added *a* connection that might not be observed by the loop, and that we dispose of
                // *a* connection in response.
                PooledConnection conn = connections.poll();
                if ( conn != null )
                {
                    conn.dispose();
                }
            }
        }
        else
        {
            pooledConnection.dispose();
        }
    }

    boolean validConnection( PooledConnection pooledConnection )
    {
        // once the pooledConn has marked to have unrecoverable errors, there is no way to remove the error
        // and we should close the conn without bothering to reset the conn at all
        return !pooledConnection.hasUnrecoverableErrors() &&
               reset(pooledConnection) &&
               (pooledConnection.idleTime() <= minIdleBeforeConnectionTest || ping( pooledConnection ));
    }

    /**
     *  In case this session has an open result or transaction or something,
     *  make sure it's reset to a nice state before we reuse it.
     * @param conn the PooledConnection
     * @return true if the connection is reset successfully without any error, otherwise false.
     */
    private boolean reset( PooledConnection conn )
    {
        try
        {
            conn.reset();
            conn.sync();
            return true;
        }
        catch ( Throwable e )
        {
            return false;
        }
    }

    private boolean ping( PooledConnection conn )
    {
        try
        {
            conn.run( "RETURN 1 // JavaDriver poll to test connection", NO_PARAMETERS, Collector.NO_OP );
            conn.pullAll( Collector.NO_OP );
            conn.sync();
            return true;
        }
        catch ( Throwable e )
        {
            return false;
        }
    }
}
