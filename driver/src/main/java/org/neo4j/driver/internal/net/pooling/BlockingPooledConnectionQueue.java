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
package org.neo4j.driver.internal.net.pooling;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.logging.DelegatingLogger;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;

/**
 * A blocking queue that also keeps track of connections that are acquired in order
 * to facilitate termination of all connections.
 */
public class BlockingPooledConnectionQueue
{
    public static final String LOG_NAME = "ConnectionQueue";

    private static final int ACTIVE = 1;
    private static final int INACTIVE = 2;
    private static final int TERMINATED = 3;

    /** The backing queue, keeps track of connections currently in queue */
    private final BlockingQueue<PooledConnection> queue;
    private final Logger logger;

    private final AtomicInteger state = new AtomicInteger( ACTIVE );

    /** Keeps track of acquired connections */
    private final Set<PooledConnection> acquiredConnections =
            Collections.newSetFromMap(new ConcurrentHashMap<PooledConnection, Boolean>());

    public BlockingPooledConnectionQueue( BoltServerAddress address, int capacity, Logging logging )
    {
        this.queue = new LinkedBlockingQueue<>( capacity );
        this.logger = createLogger( address, logging );
    }

    /**
     * Offer a connections back to the queue
     *
     * @param pooledConnection the connection to put back to the queue
     * @return <code>true</code> if connections was accepted otherwise <code>false</code>
     */
    public boolean offer( PooledConnection pooledConnection )
    {
        acquiredConnections.remove( pooledConnection );
        boolean offer = queue.offer( pooledConnection );
        // not added back to the queue, dispose of the connection
        if ( !offer )
        {
            disposeSafely( pooledConnection );
        }
        if ( state.get() != ACTIVE )
        {
            terminateIdleConnections();
        }
        return offer;
    }

    /**
     * Acquire connection or create a new one if the queue is empty
     * @param supplier used to create a new connection if queue is empty
     * @return a PooledConnection instance
     */
    public PooledConnection acquire( Supplier<PooledConnection> supplier )
    {
        PooledConnection connection = queue.poll();
        if ( connection == null )
        {
            connection = supplier.get();
        }
        acquiredConnections.add( connection );

        int poolState = state.get();
        if ( poolState != ACTIVE )
        {
            acquiredConnections.remove( connection );
            disposeSafely( connection );
            throw new IllegalStateException( "Pool is " + (poolState == INACTIVE ? "deactivated" : "terminated") +
                                             ", new connections can't be acquired" );
        }
        else
        {
            return connection;
        }
    }

    public int idleConnections()
    {
        return queue.size();
    }

    public int activeConnections()
    {
        return acquiredConnections.size();
    }

    void disposeBroken( PooledConnection connection )
    {
        acquiredConnections.remove( connection );
        disposeSafely( connection );
    }

    public boolean contains( PooledConnection pooledConnection )
    {
        return queue.contains( pooledConnection );
    }

    public void activate()
    {
        state.compareAndSet( INACTIVE, ACTIVE );
    }

    public void deactivate()
    {
        if ( state.compareAndSet( ACTIVE, INACTIVE ) )
        {
            terminateIdleConnections();
        }
    }

    public boolean isActive()
    {
        return state.get() == ACTIVE;
    }

    /**
     * Terminates all connections, both those that are currently in the queue as well
     * as those that have been acquired.
     * <p>
     * This method does not throw runtime exceptions. All connection close failures are only logged.
     */
    public void terminate()
    {
        if ( state.getAndSet( TERMINATED ) != TERMINATED )
        {
            terminateIdleConnections();
            terminateAcquiredConnections();
        }
    }

    private void terminateIdleConnections()
    {
        while ( !queue.isEmpty() )
        {
            PooledConnection idleConnection = queue.poll();
            disposeSafely( idleConnection );
        }
    }

    private void terminateAcquiredConnections()
    {
        for ( PooledConnection acquiredConnection : acquiredConnections )
        {
            disposeSafely( acquiredConnection );
        }
    }

    private void disposeSafely( PooledConnection connection )
    {
        try
        {
            if ( connection != null )
            {
                // close the underlying connection without adding it back to the queue
                connection.dispose();
            }
        }
        catch ( Throwable disposeError )
        {
            logger.error( "Error disposing connection", disposeError );
        }
    }

    private static Logger createLogger( BoltServerAddress address, Logging logging )
    {
        return new DelegatingLogger( logging.getLog( LOG_NAME ), address.toString() );
    }
}
