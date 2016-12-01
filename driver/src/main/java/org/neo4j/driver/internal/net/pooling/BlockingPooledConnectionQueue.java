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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.util.Supplier;

/**
 * A blocking queue that also keeps track of connections that are acquired in order
 * to facilitate termination of all connections.
 */
public class BlockingPooledConnectionQueue
{
    /** The backing queue, keeps track of connections currently in queue */
    private final BlockingQueue<PooledConnection> queue;

    private final AtomicBoolean isTerminating = new AtomicBoolean( false );

    /** Keeps track of acquired connections */
    private final Set<PooledConnection> acquiredConnections =
            Collections.newSetFromMap(new ConcurrentHashMap<PooledConnection, Boolean>());

    public BlockingPooledConnectionQueue( int capacity )
    {
        this.queue = new LinkedBlockingQueue<>( capacity );
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
        if (!offer) {
            pooledConnection.dispose();
        }
        if (isTerminating.get()) {
            PooledConnection poll = queue.poll();
            if (poll != null)
            {
                poll.dispose();
            }
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

        PooledConnection poll = queue.poll();
        if ( poll == null )
        {
            poll = supplier.get();
        }
        acquiredConnections.add( poll );

        if (isTerminating.get()) {
            acquiredConnections.remove( poll );
            poll.dispose();
            throw new IllegalStateException( "Pool has been closed, cannot acquire new values." );
        }
        return poll;
    }

    public List<PooledConnection> toList()
    {
        return new ArrayList<>( queue );
    }

    public boolean isEmpty()
    {
        return queue.isEmpty();
    }

    public int size()
    {
        return queue.size();
    }

    public boolean contains( PooledConnection pooledConnection )
    {
        return queue.contains( pooledConnection );
    }

    /**
     * Terminates all connections, both those that are currently in the queue as well
     * as those that have been acquired.
     */
    public void terminate()
    {
        if (isTerminating.compareAndSet( false, true ))
        {
            while ( !queue.isEmpty() )
            {
                PooledConnection conn = queue.poll();
                if ( conn != null )
                {
                    //close the underlying connection without adding it back to the queue
                    conn.dispose();
                }
            }
            for ( PooledConnection pooledConnection : acquiredConnections )
            {
                pooledConnection.dispose();
            }
        }
    }
}
