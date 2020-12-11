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
package org.neo4j.driver.stress;

import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.util.cc.ClusterMemberRole;
import org.neo4j.driver.util.cc.ClusterMemberRoleDiscoveryFactory;
import org.neo4j.driver.util.cc.LocalOrRemoteClusterExtension;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;

class CausalClusteringStressIT extends AbstractStressTestBase<CausalClusteringStressIT.Context>
{
    @RegisterExtension
    static final LocalOrRemoteClusterExtension clusterRule = new LocalOrRemoteClusterExtension();

    @Override
    URI databaseUri()
    {
        return clusterRule.getClusterUri();
    }

    @Override
    AuthToken authToken()
    {
        return clusterRule.getAuthToken();
    }

    @Override
    Config.ConfigBuilder config( Config.ConfigBuilder builder )
    {
        return clusterRule.config( builder );
    }

    @Override
    Context createContext()
    {
        return new Context();
    }

    @Override
    List<BlockingCommand<Context>> createTestSpecificBlockingCommands()
    {
        return Arrays.asList(
                new BlockingWriteQueryUsingReadSession<>( driver, false ),
                new BlockingWriteQueryUsingReadSession<>( driver, true ),
                new BlockingWriteQueryUsingReadSessionInTx<>( driver, false ),
                new BlockingWriteQueryUsingReadSessionInTx<>( driver, true )
        );
    }

    @Override
    boolean handleWriteFailure( Throwable error, Context context )
    {
        if ( error instanceof SessionExpiredException )
        {
            boolean isLeaderSwitch = error.getMessage().endsWith( "no longer accepts writes" );
            if ( isLeaderSwitch )
            {
                context.leaderSwitch();
                return true;
            }
        }
        return false;
    }

    @Override
    void assertExpectedReadQueryDistribution( Context context )
    {
        Map<String,Long> readQueriesByServer = context.getReadQueriesByServer();
        ClusterAddresses clusterAddresses = fetchClusterAddresses( driver );

        // expect all followers to serve more than zero read queries
        assertAllAddressesServedReadQueries( "Follower", clusterAddresses.followers, readQueriesByServer );

        // expect all read replicas to serve more than zero read queries
        assertAllAddressesServedReadQueries( "Read replica", clusterAddresses.readReplicas, readQueriesByServer );

        // expect all followers to serve same order of magnitude read queries
        assertAllAddressesServedSimilarAmountOfReadQueries( "Followers", clusterAddresses.followers,
                readQueriesByServer, clusterAddresses );

        // expect all read replicas to serve same order of magnitude read queries
        assertAllAddressesServedSimilarAmountOfReadQueries( "Read replicas", clusterAddresses.readReplicas,
                readQueriesByServer, clusterAddresses );
    }

    @Override
    void printStats( Context context )
    {
        System.out.println( "Nodes read: " + context.getReadNodesCount() );
        System.out.println( "Nodes created: " + context.getCreatedNodesCount() );

        System.out.println( "Leader switches: " + context.getLeaderSwitchCount() );
        System.out.println( "Bookmark failures: " + context.getBookmarkFailures() );
    }

    @Override
    void dumpLogs()
    {
        clusterRule.dumpClusterLogs();
    }

    private static ClusterAddresses fetchClusterAddresses( Driver driver )
    {
        Set<String> followers = new HashSet<>();
        Set<String> readReplicas = new HashSet<>();

        final ClusterMemberRoleDiscoveryFactory.ClusterMemberRoleDiscovery discovery =
                ClusterMemberRoleDiscoveryFactory.newInstance( ServerVersion.version( driver ) );
        final Map<BoltServerAddress,ClusterMemberRole> clusterOverview = discovery.findClusterOverview( driver );

        for ( BoltServerAddress address : clusterOverview.keySet() )
        {
            String boltAddress = String.format( "%s:%s", address.host(), address.port() );
            ClusterMemberRole role = clusterOverview.get( address );
            if ( role == ClusterMemberRole.FOLLOWER )
            {
                followers.add( boltAddress );
            }
            else if ( role == ClusterMemberRole.READ_REPLICA )
            {
                readReplicas.add( boltAddress );
            }
        }

        return new ClusterAddresses( followers, readReplicas );
    }

    private static void assertAllAddressesServedReadQueries( String addressType, Set<String> addresses,
            Map<String,Long> readQueriesByServer )
    {
        for ( String address : addresses )
        {
            Long queries = readQueriesByServer.get( address );
            assertThat( addressType + " did not serve any read queries", queries, greaterThan( 0L ) );
        }
    }

    private static void assertAllAddressesServedSimilarAmountOfReadQueries( String addressesType, Set<String> addresses,
            Map<String,Long> readQueriesByServer, ClusterAddresses allAddresses )
    {
        long expectedOrderOfMagnitude = -1;
        for ( String address : addresses )
        {
            long queries = readQueriesByServer.get( address );
            long orderOfMagnitude = orderOfMagnitude( queries );
            if ( expectedOrderOfMagnitude == -1 )
            {
                expectedOrderOfMagnitude = orderOfMagnitude;
            }
            else
            {
                assertThat( addressesType + " are expected to serve similar amount of queries. " +
                            "Addresses: " + allAddresses + ", " +
                            "read queries served: " + readQueriesByServer,
                        orderOfMagnitude,
                        both( greaterThanOrEqualTo( expectedOrderOfMagnitude - 1 ) )
                                .and( lessThanOrEqualTo( expectedOrderOfMagnitude + 1 ) ) );
            }
        }
    }

    private static long orderOfMagnitude( long number )
    {
        long result = 1;
        while ( number >= 10 )
        {
            number /= 10;
            result++;
        }
        return result;
    }

    static class Context extends AbstractContext
    {
        final ConcurrentMap<String,AtomicLong> readQueriesByServer = new ConcurrentHashMap<>();
        final AtomicInteger leaderSwitches = new AtomicInteger();

        @Override
        public void processSummary( ResultSummary summary )
        {
            if ( summary == null )
            {
                return;
            }

            String serverAddress = summary.server().address();

            //remove resolved address if present
            if ( serverAddress.contains( "(" ) )
            {
                String host = serverAddress.substring( 0, serverAddress.indexOf( "(" ) );
                serverAddress = serverAddress.replace( "(" + host + ")", "" );
            }

            AtomicLong count = readQueriesByServer.get( serverAddress );
            if ( count == null )
            {
                count = new AtomicLong();
                AtomicLong existingCounter = readQueriesByServer.putIfAbsent( serverAddress, count );
                if ( existingCounter != null )
                {
                    count = existingCounter;
                }
            }
            count.incrementAndGet();
        }

        Map<String,Long> getReadQueriesByServer()
        {
            Map<String,Long> result = new HashMap<>();
            for ( Map.Entry<String,AtomicLong> entry : readQueriesByServer.entrySet() )
            {
                result.put( entry.getKey(), entry.getValue().get() );
            }
            return result;
        }

        void leaderSwitch()
        {
            leaderSwitches.incrementAndGet();
        }

        int getLeaderSwitchCount()
        {
            return leaderSwitches.get();
        }
    }

    private static class ClusterAddresses
    {
        final Set<String> followers;
        final Set<String> readReplicas;

        ClusterAddresses( Set<String> followers, Set<String> readReplicas )
        {
            this.followers = followers;
            this.readReplicas = readReplicas;
        }

        @Override
        public String toString()
        {
            return "ClusterAddresses{" +
                   "followers=" + followers +
                   ", readReplicas=" + readReplicas +
                   '}';
        }
    }
}
