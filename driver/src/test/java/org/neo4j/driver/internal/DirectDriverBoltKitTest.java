/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.util.StubServer;

import static java.util.Arrays.asList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.StubServer.INSECURE_CONFIG;

class DirectDriverBoltKitTest
{
    @Test
    void shouldBeAbleRunCypher() throws Exception
    {
        StubServer server = StubServer.start( "return_x.script", 9001 );
        URI uri = URI.create( "bolt://127.0.0.1:9001" );
        int x;

        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ) )
        {
            try ( Session session = driver.session() )
            {
                Record record = session.run( "RETURN {x}", parameters( "x", 1 ) ).single();
                x = record.get( 0 ).asInt();
            }
        }

        assertThat( x, equalTo( 1 ) );
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldSendMultipleBookmarks() throws Exception
    {
        StubServer server = StubServer.start( "multiple_bookmarks.script", 9001 );

        List<String> bookmarks = asList( "neo4j:bookmark:v1:tx5", "neo4j:bookmark:v1:tx29",
                "neo4j:bookmark:v1:tx94", "neo4j:bookmark:v1:tx56", "neo4j:bookmark:v1:tx16",
                "neo4j:bookmark:v1:tx68" );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
              Session session = driver.session( bookmarks ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (n {name:'Bob'})" );
                tx.success();
            }

            assertEquals( "neo4j:bookmark:v1:tx95", session.lastBookmark() );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldLogConnectionIdInDebugMode() throws Exception
    {
        StubServer server = StubServer.start( "hello_run_exit.script", 9001 );

        Logger logger = mock( Logger.class );
        when( logger.isDebugEnabled() ).thenReturn( true );

        Config config = Config.build()
                .withLogging( ignore -> logger )
                .withoutEncryption().toConfig();

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", config );
              Session session = driver.session() )
        {
            List<String> names = session.run( "MATCH (n) RETURN n.name" ).list( record -> record.get( 0 ).asString() );
            assertEquals( asList( "Foo", "Bar" ), names );

            ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass( String.class );
            verify( logger, atLeastOnce() ).debug( messageCaptor.capture(), any() );

            Optional<String> logMessageWithConnectionId = messageCaptor.getAllValues()
                    .stream()
                    .filter( line -> line.contains( "bolt-123456789" ) )
                    .findAny();

            assertTrue( logMessageWithConnectionId.isPresent(),
                    "Expected log call did not happen. All debug log calls:\n" + String.join( "\n", messageCaptor.getAllValues() ) );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }
}
