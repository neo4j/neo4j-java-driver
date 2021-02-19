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

import java.io.FileNotFoundException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.util.TestUtil;
import org.neo4j.driver.util.cc.ClusterMemberRoleDiscoveryFactory.ClusterMemberRoleDiscovery;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static org.neo4j.driver.util.TestUtil.sleep;

public class Cluster implements AutoCloseable
{
    private static final String ADMIN_USER = "neo4j";
    private static final int STARTUP_TIMEOUT_SECONDS = 120;
    private static final int ONLINE_MEMBERS_CHECK_SLEEP_MS = 500;

    private final Path path;
    private final Set<ClusterMember> members;
    private final Set<ClusterMember> offlineMembers;
    private final ClusterDrivers clusterDrivers;

    public Cluster( Path path, String password )
    {
        this( path, emptySet(), new ClusterDrivers( ADMIN_USER, password ) );
    }

    private Cluster( Path path, Set<ClusterMember> members, ClusterDrivers clusterDrivers )
    {
        this.path = path;
        this.members = members;
        this.offlineMembers = new HashSet<>();
        this.clusterDrivers = clusterDrivers;
    }

    Cluster withMembers( Set<ClusterMember> newMembers ) throws ClusterUnavailableException
    {
        waitForMembersToBeOnline( newMembers, clusterDrivers );
        return new Cluster( path, newMembers, clusterDrivers );
    }

    public Path getPath()
    {
        return path;
    }

    public void deleteData()
    {
        // execute write query to remove all nodes and retrieve bookmark
        Driver driverToLeader = clusterDrivers.getDriver( leader() );
        Bookmark bookmark = TestUtil.cleanDb( driverToLeader );
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

    public void start( ClusterMember member )
    {
        startNoWait( member );
        waitForMembersToBeOnline();
    }

    public void startOfflineMembers()
    {
        // copy offline members to avoid ConcurrentModificationException
        Set<ClusterMember> currentlyOfflineMembers = new HashSet<>( offlineMembers );
        for ( ClusterMember member : currentlyOfflineMembers )
        {
            startNoWait( member );
        }
        waitForMembersToBeOnline();
    }

    public void stop( ClusterMember member )
    {
        removeOfflineMember( member );
        SharedCluster.stop( member );
        waitForMembersToBeOnline();
    }

    public void kill( ClusterMember member )
    {
        removeOfflineMember( member );
        SharedCluster.kill( member );
        waitForMembersToBeOnline();
    }

    public Driver getDirectDriver( ClusterMember member )
    {
        return clusterDrivers.getDriver( member );
    }

    public void dumpClusterDebugLog()
    {
        for ( ClusterMember member : members )
        {

            System.out.println( "Debug log for: " + member.getPath().toString() );
            try
            {
                member.dumpDebugLog();
            }
            catch ( FileNotFoundException e )
            {
                System.out.println("Unable to find debug log file for: " + member.getPath().toString());
                e.printStackTrace();
            }
        }
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
               "path=" + path +
               ", members=" + members +
               "}";
    }

    private void addOfflineMember( ClusterMember member )
    {
        if ( !offlineMembers.remove( member ) )
        {
            throw new IllegalArgumentException( "Cluster member is not offline: " + member );
        }
        members.add( member );
    }

    private void removeOfflineMember( ClusterMember member )
    {
        if ( !members.remove( member ) )
        {
            throw new IllegalArgumentException( "Unknown cluster member " + member );
        }
        offlineMembers.add( member );
    }

    private void startNoWait( ClusterMember member )
    {
        addOfflineMember( member );
        SharedCluster.start( member );
    }

    private Set<ClusterMember> membersWithRole( ClusterMemberRole role )
    {
        Set<ClusterMember> membersWithRole = new HashSet<>();
        int retryCount = 0;

        while ( membersWithRole.isEmpty() && retryCount < 10 )
        {
            Driver driver = driverToAnyCore( members, clusterDrivers );
            final ClusterMemberRoleDiscovery discovery = clusterDrivers.getDiscovery();
            final Map<BoltServerAddress,ClusterMemberRole> clusterOverview = discovery.findClusterOverview( driver );
            for ( BoltServerAddress boltAddress : clusterOverview.keySet() )
            {
                if ( role == clusterOverview.get( boltAddress ) )
                {
                    ClusterMember member = findByBoltAddress( boltAddress, members );
                    if ( member == null )
                    {
                        throw new IllegalStateException( "Unknown cluster member: '" + boltAddress + "'\n" + this );
                    }
                    membersWithRole.add( member );
                }
            }
            retryCount++;

            if ( !membersWithRole.isEmpty() )
            {
                break;
            }
            else
            {
                try
                {
                    // give some time for cluster to stabilise
                    Thread.sleep( 2000 );
                }
                catch ( InterruptedException ignored )
                {
                }
            }
        }

        if ( membersWithRole.isEmpty() )
        {
            throw new IllegalStateException( "No cluster members with role '" + role + " " + this );
        }

        return membersWithRole;
    }

    private void waitForMembersToBeOnline()
    {
        try
        {
            waitForMembersToBeOnline( members, clusterDrivers );
        }
        catch ( ClusterUnavailableException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static void waitForMembersToBeOnline( Set<ClusterMember> members, ClusterDrivers clusterDrivers )
            throws ClusterUnavailableException
    {
        if ( members.isEmpty() )
        {
            throw new IllegalArgumentException( "No members to wait for" );
        }

        Set<BoltServerAddress> expectedOnlineAddresses = extractBoltAddresses( members );
        Set<BoltServerAddress> actualOnlineAddresses = emptySet();

        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( STARTUP_TIMEOUT_SECONDS );
        Throwable error = null;

        while ( !expectedOnlineAddresses.equals( actualOnlineAddresses ) )
        {
            sleep( ONLINE_MEMBERS_CHECK_SLEEP_MS );
            assertDeadlineNotReached( deadline, expectedOnlineAddresses, actualOnlineAddresses, error );

            Driver driver = driverToAnyCore( members, clusterDrivers );
            final ClusterMemberRoleDiscovery discovery = clusterDrivers.getDiscovery();
            try
            {
                final Map<BoltServerAddress,ClusterMemberRole> clusterOverview = discovery.findClusterOverview( driver );
                actualOnlineAddresses = clusterOverview.keySet();
            }
            catch ( Throwable t )
            {
                t.printStackTrace();

                if ( error == null )
                {
                    error = t;
                }
                else
                {
                    error.addSuppressed( t );
                }
            }
        }
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
            final ClusterMemberRoleDiscovery discovery = clusterDrivers.getDiscovery();
            if ( discovery.isCoreMember( driver ) )
            {
                return driver;
            }
        }

        throw new IllegalStateException( "No core members found among: " + members );
    }

    private static void assertDeadlineNotReached( long deadline, Set<?> expectedAddresses, Set<?> actualAddresses,
            Throwable error ) throws ClusterUnavailableException
    {
        if ( System.currentTimeMillis() > deadline )
        {
            String baseMessage = "Cluster did not become available in " + STARTUP_TIMEOUT_SECONDS + " seconds.\n";
            String errorMessage = error == null ? "" : "There were errors checking cluster members.\n";
            String expectedAddressesMessage = "Expected online addresses: " + expectedAddresses + "\n";
            String actualAddressesMessage = "Actual last seen online addresses: " + actualAddresses + "\n";
            String message = baseMessage + errorMessage + expectedAddressesMessage + actualAddressesMessage;

            ClusterUnavailableException clusterUnavailable = new ClusterUnavailableException( message );

            if ( error != null )
            {
                clusterUnavailable.addSuppressed( error );
            }

            throw clusterUnavailable;
        }
    }

    private static Set<BoltServerAddress> extractBoltAddresses( Set<ClusterMember> members )
    {
        Set<BoltServerAddress> addresses = new HashSet<>();
        for ( ClusterMember member : members )
        {
            addresses.add( member.getBoltAddress() );
        }
        return addresses;
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
