/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.cluster;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;

import static java.util.Collections.EMPTY_MAP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.cluster.GetServersProcedureRunner.GET_ROUTING_TABLE;
import static org.neo4j.driver.internal.cluster.GetServersProcedureRunner.GET_ROUTING_TABLE_PARAM;
import static org.neo4j.driver.internal.cluster.GetServersProcedureRunner.GET_SERVERS;
import static org.neo4j.driver.v1.Values.parameters;

public class GetServersProcedureRunnerTest
{
    @Test
    public void shouldCallGetRoutingTableWithEmptyMap() throws Throwable
    {
        // Given
        GetServersProcedureRunner runner = new TestGetServersProcedureRunner( EMPTY_MAP );
        Connection mock = mock( Connection.class );
        when( mock.server() ).thenReturn(
                new InternalServerInfo( new BoltServerAddress( "123:45" ), "Neo4j/3.2.1" ) );
        // When
        runner.run( mock );

        // Then
        assertThat( runner.procedureCalled(), equalTo(
                new Statement( "CALL " + GET_ROUTING_TABLE, parameters( GET_ROUTING_TABLE_PARAM, EMPTY_MAP ) ) ) );
    }

    @Test
    public void shouldCallGetRoutingTableWithParam() throws Throwable
    {
        // Given
        HashMap<String,String> param = new HashMap<>();
        param.put( "key1", "value1" );
        param.put( "key2", "value2" );
        GetServersProcedureRunner runner = new TestGetServersProcedureRunner( param );
        Connection mock = mock( Connection.class );
        when( mock.server() ).thenReturn(
                new InternalServerInfo( new BoltServerAddress( "123:45" ), "Neo4j/3.2.1" ) );
        // When
        runner.run( mock );

        // Then
        assertThat( runner.procedureCalled(), equalTo(
                new Statement( "CALL " + GET_ROUTING_TABLE, parameters( GET_ROUTING_TABLE_PARAM, param ) ) ) );
    }

    @Test
    public void shouldCallGetServers() throws Throwable
    {
        // Given
        HashMap<String,String> param = new HashMap<>();
        param.put( "key1", "value1" );
        param.put( "key2", "value2" );
        GetServersProcedureRunner runner = new TestGetServersProcedureRunner( param );
        Connection mock = mock( Connection.class );
        when( mock.server() ).thenReturn(
                new InternalServerInfo( new BoltServerAddress( "123:45" ), "Neo4j/3.1.8" ) );
        // When
        runner.run( mock );

        // Then
        assertThat( runner.procedureCalled(), equalTo(
                new Statement( "CALL " + GET_SERVERS ) ) );
    }

    private static class TestGetServersProcedureRunner extends GetServersProcedureRunner
    {

        TestGetServersProcedureRunner( Map<String, String> parameters )
        {
            super( parameters );
        }

        @Override
        List<Record> runProcedure( Connection connection, Statement procedure )
        {
            // I do not want any network traffic
            return null;
        }
    }

}
