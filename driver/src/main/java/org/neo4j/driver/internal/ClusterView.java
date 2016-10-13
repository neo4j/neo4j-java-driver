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
package org.neo4j.driver.internal;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.ConcurrentRoundRobinSet;
import org.neo4j.driver.v1.Logger;

/**
 * Defines a snapshot view of the cluster.
 */
class ClusterView
{
    private final static Comparator<BoltServerAddress> COMPARATOR = new Comparator<BoltServerAddress>()
    {
        @Override
        public int compare( BoltServerAddress o1, BoltServerAddress o2 )
        {
            int compare = o1.host().compareTo( o2.host() );
            if ( compare == 0 )
            {
                compare = Integer.compare( o1.port(), o2.port() );
            }

            return compare;
        }
    };

    private static final int MIN_ROUTERS = 1;

    private final ConcurrentRoundRobinSet<BoltServerAddress> routingServers =
            new ConcurrentRoundRobinSet<>( COMPARATOR );
    private final ConcurrentRoundRobinSet<BoltServerAddress> readServers =
            new ConcurrentRoundRobinSet<>( COMPARATOR );
    private final ConcurrentRoundRobinSet<BoltServerAddress> writeServers =
            new ConcurrentRoundRobinSet<>( COMPARATOR );
    private final Clock clock;
    private final long expires;
    private final Logger log;

    public ClusterView( long expires, Clock clock, Logger log )
    {
        this.expires = expires;
        this.clock = clock;
        this.log = log;
    }

    public void addRouter( BoltServerAddress router )
    {
        this.routingServers.add( router );
    }

    public boolean isStale()
    {
        return expires < clock.millis() ||
               routingServers.size() <= MIN_ROUTERS ||
               readServers.isEmpty() ||
               writeServers.isEmpty();
    }

    Set<BoltServerAddress> all()
    {
        HashSet<BoltServerAddress> all =
                new HashSet<>( routingServers.size() + readServers.size() + writeServers.size() );
        all.addAll( routingServers );
        all.addAll( readServers );
        all.addAll( writeServers );
        return all;
    }


    public BoltServerAddress nextRouter()
    {
        return routingServers.hop();
    }

    public BoltServerAddress nextReader()
    {
        return readServers.hop();
    }

    public BoltServerAddress nextWriter()
    {
        return writeServers.hop();
    }

    public void addReaders( List<BoltServerAddress> addresses )
    {
        readServers.addAll( addresses );
    }

    public void addWriters( List<BoltServerAddress> addresses )
    {
        writeServers.addAll( addresses );
    }

    public void addRouters( List<BoltServerAddress> addresses )
    {
        routingServers.addAll( addresses );
    }

    public void remove( BoltServerAddress address )
    {
        if ( routingServers.remove( address ) )
        {
            log.debug( "Removing %s from routers", address.toString() );
        }
        if ( readServers.remove( address ) )
        {
            log.debug( "Removing %s from readers", address.toString() );
        }
        if ( writeServers.remove( address ) )
        {
            log.debug( "Removing %s from writers", address.toString() );
        }
    }

    public boolean removeWriter( BoltServerAddress address )
    {
        return writeServers.remove( address );
    }

    public int numberOfRouters()
    {
        return routingServers.size();
    }

    public int numberOfReaders()
    {
        return readServers.size();
    }

    public int numberOfWriters()
    {
        return writeServers.size();
    }

    public Set<BoltServerAddress> routingServers()
    {
        return Collections.unmodifiableSet( routingServers );
    }

    public Set<BoltServerAddress> readServers()
    {
        return Collections.unmodifiableSet( readServers );
    }

    public Set<BoltServerAddress> writeServers()
    {
        return Collections.unmodifiableSet( writeServers );
    }
}
