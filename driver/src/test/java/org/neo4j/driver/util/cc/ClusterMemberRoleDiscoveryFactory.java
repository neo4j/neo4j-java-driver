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
package org.neo4j.driver.util.cc;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.util.ServerVersion;

import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.internal.util.Iterables.single;

public class ClusterMemberRoleDiscoveryFactory
{
    public static ClusterMemberRoleDiscovery newInstance( ServerVersion version )
    {
        if ( version.greaterThanOrEqual( ServerVersion.v4_0_0 ) )
        {
            return new SimpleClusterMemberRoleDiscovery();
        }
        else
        {
            return new LegacyClusterMemberRoleDiscovery();
        }
    }

    public interface ClusterMemberRoleDiscovery
    {
        boolean isCoreMember( Driver driver );

        Map<BoltServerAddress,ClusterMemberRole> findClusterOverview( Driver driver );
    }

    public static class LegacyClusterMemberRoleDiscovery implements ClusterMemberRoleDiscovery
    {
        @Override
        public boolean isCoreMember( Driver driver )
        {
            try ( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() ) )
            {
                Record record = single( session.run( "CALL dbms.cluster.role()" ).list() );
                ClusterMemberRole role = extractRole( record );
                return role == ClusterMemberRole.LEADER || role == ClusterMemberRole.FOLLOWER;
            }
        }

        @Override
        public Map<BoltServerAddress,ClusterMemberRole> findClusterOverview( Driver driver )
        {
            try ( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).build() ) )
            {
                Result result = session.run( "CALL dbms.cluster.overview()" );
                Map<BoltServerAddress,ClusterMemberRole> overview = new HashMap<>();
                for ( Record record : result.list() )
                {
                    final BoltServerAddress address = extractBoltAddress( record );
                    final ClusterMemberRole role = extractRole( record );
                    overview.put( address, role );
                }
                return overview;
            }
        }
    }

    public static class SimpleClusterMemberRoleDiscovery implements ClusterMemberRoleDiscovery
    {
        private static final String DEFAULT_DATABASE = "neo4j";

        @Override
        public boolean isCoreMember( Driver driver )
        {
            try ( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() ) )
            {
                Record record = single( session.run( "CALL dbms.cluster.role($database)",
                        parameters( "database", DEFAULT_DATABASE ) ).list() );
                ClusterMemberRole role = extractRole( record );
                return role == ClusterMemberRole.LEADER || role == ClusterMemberRole.FOLLOWER;
            }
        }

        @Override
        public Map<BoltServerAddress,ClusterMemberRole> findClusterOverview( Driver driver )
        {
            try ( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() ) )
            {
                Result result = session.run( "CALL dbms.cluster.overview()" );
                Map<BoltServerAddress,ClusterMemberRole> overview = new HashMap<>();
                for ( Record record : result.list() )
                {
                    final BoltServerAddress address = extractBoltAddress( record );
                    final ClusterMemberRole role = extractRoleForDatabase( record, DEFAULT_DATABASE );
                    if ( role != ClusterMemberRole.UNKNOWN ) // the unknown ones has not fully come online
                    {
                        overview.put( address, role );
                    }
                }
                return overview;
            }
        }
    }

    private static ClusterMemberRole extractRoleForDatabase( Record record, String database )
    {
        final Map<String,String> databases = record.get( "databases" ).asMap( Values.ofString() );
        final String roleString = databases.get( database );
        return ClusterMemberRole.valueOf( roleString.toUpperCase() );
    }

    private static BoltServerAddress extractBoltAddress( Record record )
    {
        List<Object> addresses = record.get( "addresses" ).asList();
        String boltUriString = (String) addresses.get( 0 );
        URI boltUri = URI.create( boltUriString );
        return newBoltServerAddress( boltUri );
    }

    private static BoltServerAddress newBoltServerAddress( URI uri )
    {
        try
        {
            return new BoltServerAddress( uri ).resolve();
        }
        catch ( UnknownHostException e )
        {
            throw new RuntimeException( "Unable to resolve host to IP in URI: '" + uri + "'" );
        }
    }

    private static ClusterMemberRole extractRole( Record record )
    {
        String roleString = record.get( "role" ).asString();
        return ClusterMemberRole.valueOf( roleString.toUpperCase() );
    }
}
