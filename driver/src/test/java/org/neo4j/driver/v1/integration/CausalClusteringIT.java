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
package org.neo4j.driver.v1.integration;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.cc.Cluster;
import org.neo4j.driver.v1.util.cc.ClusterMember;
import org.neo4j.driver.v1.util.cc.ClusterRule;

public class CausalClusteringIT
{
    private static final long DEFAULT_TIMEOUT_MS = 15_000;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule();

    @Test
    public void shouldExecuteReadAndWritesWhenDriverSuppliedWithAddressOfLeader() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();

        ClusterMember leader = cluster.leaderTx( new Consumer<Session>()
        {
            @Override
            public void accept( Session session )
            {
                session.run( "CREATE CONSTRAINT ON (p:Person) ASSERT p.name is UNIQUE" );
            }
        } );

        executeWriteAndReadThroughBolt( leader );
    }

    private int executeWriteAndReadThroughBolt( ClusterMember member ) throws TimeoutException, InterruptedException
    {
        try ( Driver driver = GraphDatabase.driver( member.getRoutingUri(), clusterRule.getDefaultAuthToken() ) )
        {
            return inExpirableSession( driver, createWritableSession(), executeWriteAndRead() );
        }
    }

    private Function<Driver,Session> createWritableSession()
    {
        return new Function<Driver,Session>()
        {
            @Override
            public Session apply( Driver driver )
            {
                return driver.session( AccessMode.WRITE );
            }
        };
    }

    private Function<Session,Integer> executeWriteAndRead()
    {
        return new Function<Session,Integer>()
        {
            @Override
            public Integer apply( Session session )
            {
                session.run( "MERGE (n:Person {name: 'Jim'})" ).consume();
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                return record.get( "count" ).asInt();
            }
        };
    }

    private <T> T inExpirableSession( Driver driver, Function<Driver,Session> acquirer, Function<Session,T> op )
            throws TimeoutException, InterruptedException
    {
        long endTime = System.currentTimeMillis() + DEFAULT_TIMEOUT_MS;

        do
        {
            try ( Session session = acquirer.apply( driver ) )
            {
                return op.apply( session );
            }
            catch ( SessionExpiredException e )
            {
                // role might have changed; try again;
            }
        }
        while ( System.currentTimeMillis() < endTime );

        throw new TimeoutException( "Transaction did not succeed in time" );
    }
}
