/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.driver.internal.util;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.cluster.ClusterComposition;

public final class ClusterCompositionUtil
{
    private ClusterCompositionUtil()
    {
    }

    public static final long NEVER_EXPIRE = System.currentTimeMillis() + TimeUnit.HOURS.toMillis( 1 );

    public static final BoltServerAddress A = new BoltServerAddress( "192.168.99.1:11" );
    public static final BoltServerAddress B = new BoltServerAddress( "192.168.99.2:22" );
    public static final BoltServerAddress C = new BoltServerAddress( "192.168.99.3:33" );
    public static final BoltServerAddress D = new BoltServerAddress( "192.168.99.4:44" );
    public static final BoltServerAddress E = new BoltServerAddress( "192.168.99.5:55" );
    public static final BoltServerAddress F = new BoltServerAddress( "192.168.99.6:66" );

    public static BoltServerAddress A_RESOLVED;
    public static BoltServerAddress B_RESOLVED;
    public static BoltServerAddress C_RESOLVED;
    public static BoltServerAddress D_RESOLVED;
    public static BoltServerAddress E_RESOLVED;
    public static BoltServerAddress F_RESOLVED;

    static
    {
        try
        {
            A_RESOLVED = A.resolve();
            B_RESOLVED = B.resolve();
            C_RESOLVED = C.resolve();
            D_RESOLVED = D.resolve();
            E_RESOLVED = E.resolve();
            F_RESOLVED = F.resolve();
        }
        catch ( UnknownHostException e )
        {
            throw new RuntimeException( e );
        }
    }

    public static final List<BoltServerAddress> EMPTY = new ArrayList<>();

    @SafeVarargs
    public static ClusterComposition createClusterComposition( List<BoltServerAddress>... servers )
    {
        return createClusterComposition( NEVER_EXPIRE, servers );
    }

    @SafeVarargs
    public static ClusterComposition createClusterComposition( long expirationTimestamp, List<BoltServerAddress>...
            servers )
    {
        Set<BoltServerAddress> routers = new LinkedHashSet<>();
        Set<BoltServerAddress> writers = new LinkedHashSet<>();
        Set<BoltServerAddress> readers = new LinkedHashSet<>();

        switch( servers.length )
        {
        case 3:
            readers.addAll( servers[2] );
            // no break on purpose
        case 2:
            writers.addAll( servers[1] );
            // no break on purpose
        case 1:
            routers.addAll( servers[0] );
        }
        return new ClusterComposition( expirationTimestamp, readers, writers, routers );
    }
}
