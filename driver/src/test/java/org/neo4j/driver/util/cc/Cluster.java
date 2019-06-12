/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.util.TestUtil;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static org.neo4j.driver.internal.util.Iterables.single;

public class Cluster implements AutoCloseable
{
    private final Set<ClusterMember> members;
    private final ClusterDrivers clusterDrivers;

    public Cluster( String user, String password )
    {
        this( emptySet(), new ClusterDrivers( user, password ) );
    }

    private Cluster( Set<ClusterMember> members, ClusterDrivers clusterDrivers )
    {
        this.members = members;
        this.clusterDrivers = clusterDrivers;
    }

    Cluster withMembers( Set<ClusterMember> newMembers )
    {
        return new Cluster( newMembers, clusterDrivers );
    }

    public void deleteData()
    {
        // execute write query to remove all nodes and retrieve bookmark
        Driver driverToLeader = clusterDrivers.getDriver( leader() );
        String bookmark = TestUtil.cleanDb( driverToLeader );
        if ( bookmark == null )
        {
            throw new IllegalStateException( "Cleanup of the database did not produce a bookmark" );
        }

        // ensure that every cluster member is up-to-date and contains no nodes
        for ( ClusterMember member : members )
        {
            Driver driver = clusterDrivers.getDriver( member );
            long nodeCount = TestUtil.countNodes( driver, bookmark );
            if ( nodeCount != 0 )
            {
                throw new IllegalStateException( "Not all nodes have been deleted. " + nodeCount + " still there somehow" );
            }
        }
    }

    public Set<ClusterMember> members()
    {
        return unmodifiableSet( members );
    }

    public ClusterMember leader()
    {
        Set<ClusterMember> leaders = membersWithRole( ClusterMemberRole.LEADER );
        if ( leaders.size() != 1 )
        {
            throw new IllegalStateException( "Single leader expected. " + leaders );
        }
        return leaders.iterator().next();
    }

    public ClusterMember anyFollower()
    {
        return randomOf( followers() );
    }

    public Set<ClusterMember> followers()
    {
        return membersWithRole( ClusterMemberRole.FOLLOWER );
    }

    public ClusterMember anyReadReplica()
    {
        return randomOf( readReplicas() );
    }

    public Set<ClusterMember> cores()
    {
        Set<ClusterMember> readReplicas = membersWithRole( ClusterMemberRole.READ_REPLICA );
        Set<ClusterMember> cores = new HashSet<>( members );
        cores.removeAll( readReplicas );
        return cores;
    }

    public Set<ClusterMember> readReplicas()
    {
        return membersWithRole( ClusterMemberRole.READ_REPLICA );
    }

    public Driver getDirectDriver( ClusterMember member )
    {
        return clusterDrivers.getDriver( member );
    }

    @Override
    public void close()
    {
        clusterDrivers.close();
    }

    @Override
    public String toString()
    {
        return "Cluster{" +
               "members=" + members +
               "}";
    }

    private Set<ClusterMember> membersWithRole( ClusterMemberRole role )
    {
        Set<ClusterMember> membersWithRole = new HashSet<>();

        Driver driver = driverToAnyCore( members, clusterDrivers );
        try ( Session session = driver.session( t -> t.withDefaultAccessMode( AccessMode.READ ) ) )
        {
            List<Record> records = findClusterOverview( session );
            for ( Record record : records )
            {
                if ( role == extractRole( record ) )
                {
                    BoltServerAddress boltAddress = extractBoltAddress( record );
                    ClusterMember member = findByBoltAddress( boltAddress, members );
                    if ( member == null )
                    {
                        throw new IllegalStateException( "Unknown cluster member: '" + boltAddress + "'\n" + this );
                    }
                    membersWithRole.add( member );
                }
            }
        }

        if ( membersWithRole.isEmpty() )
        {
            throw new IllegalStateException( "No cluster members with role '" + role + "' found.\n" + this );
        }

        return membersWithRole;
    }

    private static Driver driverToAnyCore( Set<ClusterMember> members, ClusterDrivers clusterDrivers )
    {
        if ( members.isEmpty() )
        {
            throw new IllegalArgumentException( "No members, can't create driver" );
        }

        for ( ClusterMember member : members )
        {
            Driver driver = clusterDrivers.getDriver( member );
            try ( Session session = driver.session( t -> t.withDefaultAccessMode( AccessMode.READ ) ) )
            {
                if ( isCoreMember( session ) )
                {
                    return driver;
                }
            }
        }

        throw new IllegalStateException( "No core members found among: " + members );
    }

    private static List<Record> findClusterOverview( Session session )
    {
        StatementResult result = session.run( "CALL dbms.cluster.overview()" );
        return result.list();
    }

    private static boolean isCoreMember( Session session )
    {
        Record record = single( session.run( "call dbms.cluster.role()" ).list() );
        ClusterMemberRole role = extractRole( record );
        return role != ClusterMemberRole.READ_REPLICA;
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

    private static ClusterMember findByBoltAddress( BoltServerAddress boltAddress, Set<ClusterMember> members )
    {
        for ( ClusterMember member : members )
        {
            if ( member.getBoltAddress().equals( boltAddress ) )
            {
                return member;
            }
        }
        return null;
    }

    private static ClusterMember randomOf( Set<ClusterMember> members )
    {
        int randomIndex = ThreadLocalRandom.current().nextInt( members.size() );
        int currentIndex = 0;
        for ( ClusterMember member : members )
        {
            if ( currentIndex == randomIndex )
            {
                return member;
            }
            currentIndex++;
        }
        throw new AssertionError();
    }
}
