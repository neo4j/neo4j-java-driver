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
package org.neo4j.driver.v1.util.cc;

import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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

import static org.neo4j.driver.v1.Config.TrustStrategy.trustAllCertificates;

public class Cluster
{
    private static final String ADMIN_USER = "neo4j";

    private final Path path;
    private final String password;
    private final Set<ClusterMember> members;

    public Cluster( Path path, String password )
    {
        this( path, password, Collections.<ClusterMember>emptySet() );
    }

    public Cluster( Path path, String password, Set<ClusterMember> members )
    {
        this.path = Objects.requireNonNull( path );
        this.password = password;
        this.members = createMembers( password, members );
    }

    Cluster withMembers( Set<ClusterMember> newMembers )
    {
        return new Cluster( path, password, newMembers );
    }

    public Path getPath()
    {
        return path;
    }

    public ClusterMember leaderTx( Consumer<Session> tx )
    {
        // todo: handle leader switches
        ClusterMember leader = findLeader();
        try ( Driver driver = createDriver( leader.getBoltUri(), password );
              Session session = driver.session() )
        {
            tx.accept( session );
        }

        return leader;
    }

    @Override
    public String toString()
    {
        return "Cluster{" +
               "path=" + path +
               ", members=" + members +
               "}";
    }

    private static Set<ClusterMember> createMembers( String password, Set<ClusterMember> givenMembers )
    {
        if ( givenMembers.isEmpty() )
        {
            return givenMembers;
        }

        Set<ClusterMember> expectedMembers = new HashSet<>( givenMembers );
        Set<ClusterMember> actualMembers = new HashSet<>();

        try ( Driver driver = createDriver( password, givenMembers ) )
        {
            // todo: add some timeout
            while ( !expectedMembers.isEmpty() )
            {
                try ( Session session = driver.session( AccessMode.READ ) )
                {
                    StatementResult result = session.run( "call dbms.cluster.overview()" );
                    for ( Record record : result.list() )
                    {
                        URI boltUri = extractBoltUri( record );

                        ClusterMember member = findByBoltUri( boltUri, expectedMembers );
                        if ( member != null )
                        {
                            ClusterMemberRole role = extractRole( record );
                            ClusterMember actualMember = member.withRole( role );
                            actualMembers.add( actualMember );
                            expectedMembers.remove( member );
                        }
                    }
                }
            }
        }


        return actualMembers;
    }

    private ClusterMember findLeader()
    {
        try ( Driver driver = createDriver( password, members );
              Session session = driver.session( AccessMode.READ ) )
        {
            StatementResult result = session.run( "call dbms.cluster.overview()" );
            for ( Record record : result.list() )
            {
                ClusterMemberRole role = extractRole( record );
                if ( role == ClusterMemberRole.LEADER )
                {
                    URI boltUri = extractBoltUri( record );
                    ClusterMember leader = findByBoltUri( boltUri, members );
                    if ( leader == null )
                    {
                        throw new IllegalStateException( "Unknown leader: '" + boltUri + "'\n" + this );
                    }
                    return leader;
                }
            }
        }
        throw new IllegalStateException( "Unable to find leader.\n" + this );
    }

    private static Driver createDriver( String password, Set<ClusterMember> members )
    {
        if ( members.isEmpty() )
        {
            throw new IllegalArgumentException( "No members, can't create driver" );
        }

        ClusterMember firstMember = members.iterator().next();
        URI boltUri = firstMember.getBoltUri();
        return createDriver( boltUri, password );
    }

    private static Driver createDriver( URI boltUri, String password )
    {
        return GraphDatabase.driver( boltUri, AuthTokens.basic( ADMIN_USER, password ), driverConfig() );
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
}
