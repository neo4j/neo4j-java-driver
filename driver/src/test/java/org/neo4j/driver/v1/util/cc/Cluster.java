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
package org.neo4j.driver.v1.util.cc;

import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.v1.Config.TrustStrategy.trustAllCertificates;

public class Cluster
{
    private static final String ADMIN_USER = "neo4j";
    private static final int STARTUP_TIMEOUT_SECONDS = 60;

    private final Path path;
    private final String password;
    private final Set<ClusterMember> members;

    public Cluster( Path path, String password )
    {
        this( path, password, Collections.<ClusterMember>emptySet() );
    }

    private Cluster( Path path, String password, Set<ClusterMember> members )
    {
        this.path = path;
        this.password = password;
        this.members = members;
    }

    Cluster withMembers( Set<ClusterMember> newMembers ) throws ClusterUnavailableException
    {
        waitForMembers( newMembers, password );
        return new Cluster( path, password, newMembers );
    }

    public Path getPath()
    {
        return path;
    }

    public void cleanUp()
    {
        leaderTx( new Consumer<Session>()
        {
            @Override
            public void accept( Session session )
            {
                session.run( "MATCH (n) DETACH DELETE n" ).consume();
            }
        } );
    }

    public ClusterMember leaderTx( Consumer<Session> tx )
    {
        ClusterMember leader = leader();
        try ( Driver driver = createDriver( leader.getBoltUri(), password );
              Session session = driver.session() )
        {
            tx.accept( session );
        }

        return leader;
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

    public Set<ClusterMember> readReplicas()
    {
        return membersWithRole( ClusterMemberRole.READ_REPLICA );
    }

    @Override
    public String toString()
    {
        return "Cluster{" +
               "path=" + path +
               ", members=" + members +
               "}";
    }

    private Set<ClusterMember> membersWithRole( ClusterMemberRole role )
    {
        Set<ClusterMember> membersWithRole = new HashSet<>();

        try ( Driver driver = createDriver( members, password );
              Session session = driver.session( AccessMode.READ ) )
        {
            StatementResult result = session.run( "call dbms.cluster.overview()" );
            for ( Record record : result.list() )
            {
                if ( role == extractRole( record ) )
                {
                    URI boltUri = extractBoltUri( record );
                    ClusterMember member = findByBoltUri( boltUri, members );
                    if ( member == null )
                    {
                        throw new IllegalStateException( "Unknown cluster member: '" + boltUri + "'\n" + this );
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

    private static Set<ClusterMember> waitForMembers( Set<ClusterMember> members, String password )
            throws ClusterUnavailableException
    {
        if ( members.isEmpty() )
        {
            throw new IllegalArgumentException( "No members to wait for" );
        }

        Set<ClusterMember> offlineMembers = new HashSet<>( members );
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( STARTUP_TIMEOUT_SECONDS );

        try ( Driver driver = createDriver( members, password ) )
        {
            while ( !offlineMembers.isEmpty() )
            {
                assertDeadlineNotReached( deadline );

                try ( Session session = driver.session( AccessMode.READ ) )
                {
                    StatementResult result = session.run( "call dbms.cluster.overview()" );
                    for ( Record record : result.list() )
                    {
                        URI boltUri = extractBoltUri( record );

                        ClusterMember member = findByBoltUri( boltUri, offlineMembers );
                        if ( member != null )
                        {
                            offlineMembers.remove( member );
                        }
                    }
                }
            }
        }

        return members;
    }

    private static Driver createDriver( Set<ClusterMember> members, String password )
    {
        if ( members.isEmpty() )
        {
            throw new IllegalArgumentException( "No members, can't create driver" );
        }

        for ( ClusterMember member : members )
        {
            Driver driver = createDriver( member.getBoltUri(), password );
            try ( Session session = driver.session( AccessMode.READ ) )
            {
                if ( isCoreMember( session ) )
                {
                    return driver;
                }
            }
            catch ( Exception e )
            {
                driver.close();
                throw e;
            }
        }

        throw new IllegalStateException( "No core members found among: " + members );
    }

    private static Driver createDriver( URI boltUri, String password )
    {
        return GraphDatabase.driver( boltUri, AuthTokens.basic( ADMIN_USER, password ), driverConfig() );
    }

    private static boolean isCoreMember( Session session )
    {
        Record record = single( session.run( "call dbms.cluster.role" ).list() );
        String roleName = record.get( "role" ).asString();
        ClusterMemberRole role = ClusterMemberRole.valueOf( roleName.toUpperCase() );
        return role != ClusterMemberRole.READ_REPLICA;
    }

    private static void assertDeadlineNotReached( long deadline ) throws ClusterUnavailableException
    {
        if ( System.currentTimeMillis() > deadline )
        {
            throw new ClusterUnavailableException(
                    "Cluster did not become available in " + STARTUP_TIMEOUT_SECONDS + " seconds" );
        }
    }

    private static URI extractBoltUri( Record record )
    {
        List<Object> addresses = record.get( "addresses" ).asList();
        String boltUriString = (String) addresses.get( 0 );
        return URI.create( boltUriString );
    }

    private static ClusterMemberRole extractRole( Record record )
    {
        String roleString = record.get( "role" ).asString();
        return ClusterMemberRole.valueOf( roleString.toUpperCase() );
    }

    private static ClusterMember findByBoltUri( URI boltUri, Set<ClusterMember> members )
    {
        for ( ClusterMember member : members )
        {
            if ( member.getBoltUri().equals( boltUri ) )
            {
                return member;
            }
        }
        return null;
    }

    private static Config driverConfig()
    {
        // try to build config for a very lightweight driver
        return Config.build()
                .withTrustStrategy( trustAllCertificates() )
                .withEncryptionLevel( Config.EncryptionLevel.NONE )
                .withMaxIdleSessions( 1 )
                .withSessionLivenessCheckTimeout( TimeUnit.HOURS.toMillis( 1 ) )
                .toConfig();
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
