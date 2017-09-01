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
package org.neo4j.driver.v1.integration;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.internal.netty.StatementResultCursor;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class SessionAsyncIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldRunQueryWithEmptyResult()
    {
        StatementResultCursor cursor = session.runAsync( "CREATE (:Person)" );

        assertThat( await( cursor.fetchAsync() ), is( false ) );
    }

    @Test
    public void shouldRunQueryWithSingleResult()
    {
        StatementResultCursor cursor = session.runAsync( "CREATE (p:Person {name: 'Nick Fury'}) RETURN p" );

        assertThat( await( cursor.fetchAsync() ), is( true ) );

        Record record = cursor.current();
        Node node = record.get( 0 ).asNode();
        assertEquals( "Person", single( node.labels() ) );
        assertEquals( "Nick Fury", node.get( "name" ).asString() );

        assertThat( await( cursor.fetchAsync() ), is( false ) );
    }

    @Test
    public void shouldRunQueryWithMultipleResults()
    {
        StatementResultCursor cursor = session.runAsync( "UNWIND [1,2,3] AS x RETURN x" );

        assertThat( await( cursor.fetchAsync() ), is( true ) );
        assertEquals( 1, cursor.current().get( 0 ).asInt() );

        assertThat( await( cursor.fetchAsync() ), is( true ) );
        assertEquals( 2, cursor.current().get( 0 ).asInt() );

        assertThat( await( cursor.fetchAsync() ), is( true ) );
        assertEquals( 3, cursor.current().get( 0 ).asInt() );

        assertThat( await( cursor.fetchAsync() ), is( false ) );
    }

    @Test
    public void shouldFailForIncorrectQuery()
    {
        StatementResultCursor cursor = session.runAsync( "RETURN" );

        try
        {
            await( cursor.fetchAsync() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertSyntaxError( e );
        }
    }

    @Test
    public void shouldFailWhenQueryFailsAtRuntime()
    {
        StatementResultCursor cursor = session.runAsync( "UNWIND [1, 2, 0] AS x RETURN 10 / x" );

        assertThat( await( cursor.fetchAsync() ), is( true ) );
        assertEquals( 10, cursor.current().get( 0 ).asInt() );

        assertThat( await( cursor.fetchAsync() ), is( true ) );
        assertEquals( 5, cursor.current().get( 0 ).asInt() );

        try
        {
            await( cursor.fetchAsync() );
            System.out.println( cursor.current() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertArithmeticError( e );
        }
    }

    @Test
    public void shouldFailWhenServerIsRestarted() throws Exception
    {
        StatementResultCursor cursor = session.runAsync(
                "UNWIND range(0, 1000000) AS x " +
                "CREATE (n1:Node {value: x})-[r:LINKED {value: x}]->(n2:Node {value: x}) " +
                "DETACH DELETE n1, n2 " +
                "RETURN x" );

        int processedRecords = 0;
        try
        {
            boolean shouldStopDb = true;
            while ( await( cursor.fetchAsync() ) )
            {
                if ( shouldStopDb )
                {
                    session.killDb();
                    shouldStopDb = false;
                }

                assertNotNull( cursor.current() );
                processedRecords++;
            }
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        }

        System.out.println( "processedRecords = " + processedRecords );
    }

    private static void assertSyntaxError( Exception e )
    {
        assertThat( e, instanceOf( ClientException.class ) );
        assertThat( ((ClientException) e).code(), containsString( "SyntaxError" ) );
        assertThat( e.getMessage(), startsWith( "Unexpected end of input" ) );
    }

    private static void assertArithmeticError( Exception e )
    {
        assertThat( e, instanceOf( ClientException.class ) );
        assertThat( ((ClientException) e).code(), containsString( "ArithmeticError" ) );
    }
}
