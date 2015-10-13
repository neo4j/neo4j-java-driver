/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.integration;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.ReusableResult;
import org.neo4j.driver.Values;
import org.neo4j.driver.util.TestSession;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.driver.Values.parameters;

public class StatementIT
{
    @Rule
    public TestSession session = new TestSession();

    @Test
    public void shouldRun() throws Throwable
    {
        // When
        session.run( "CREATE (n:FirstNode)" );

        // Then nothing should've failed
    }

    @Test
    public void shouldRunSimpleStatement() throws Throwable
    {
        // When I run a simple write statement
        session.run( "CREATE (a {name:'Adam'})" );

        // And I run a read statement
        Result result = session.run( "MATCH (a) RETURN a.name" );

        // Then I expect to get the name back
        String name = null;
        while ( result.next() )
        {
            name = result.get( "a.name" ).javaString();
        }

        assertThat( name, equalTo( "Adam" ) );
    }

    @Test
    public void shouldRunWithResult() throws Throwable
    {
        // When I execute a statement that yields a result
        ReusableResult result = session.run( "UNWIND [1,2,3] AS k RETURN k" ).retain();

        // Then the result object should contain the returned values
        assertThat( result.size(), equalTo( 3l ) );

        // And it should allow random access
        assertThat( result.get( 0 ).get( "k" ).javaLong(), equalTo( 1l ) );
        assertThat( result.get( 1 ).get( "k" ).javaLong(), equalTo( 2l ) );
        assertThat( result.get( 2 ).get( "k" ).javaLong(), equalTo( 3l ) );

        // And it should allow iteration
        long expected = 0;
        for ( Record value : result )
        {
            expected += 1;
            assertThat( value.get( "k" ), equalTo( Values.value( expected ) ) );
        }
        assertThat( expected, equalTo( 3l ) );
    }

    @Test
    public void shouldRunWithParameters() throws Throwable
    {
        // When
        session.run( "CREATE (n:FirstNode {name:{name}})", parameters( "name", "Steven" ) );

        // Then nothing should've failed
    }

    @Test
    public void shouldRunParameterizedWithResult() throws Throwable
    {
        // When
        ReusableResult result =
                session.run( "UNWIND {list} AS k RETURN k", parameters( "list", asList( 1, 2, 3 ) ) ).retain();

        // Then
        assertThat( result.size(), equalTo( 3l ) );
    }
}
