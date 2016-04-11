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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;

public class StatementIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldRunWithResult() throws Throwable
    {
        // When I execute a statement that yields a result
        List<Record> result = session.run( "UNWIND [1,2,3] AS k RETURN k" ).list();

        // Then the result object should contain the returned values
        assertThat( result.size(), equalTo( 3 ) );

        // And it should allow random access
        assertThat( result.get( 0 ).get( "k" ).asLong(), equalTo( 1L ) );
        assertThat( result.get( 1 ).get( "k" ).asLong(), equalTo( 2L ) );
        assertThat( result.get( 2 ).get( "k" ).asLong(), equalTo( 3L ) );

        // And it should allow iteration
        long expected = 0;
        for ( Record value : result )
        {
            expected += 1;
            assertThat( value.get( "k" ), equalTo( Values.value( expected ) ) );
        }
        assertThat( expected, equalTo( 3L ) );
    }

    @Test
    public void shouldRunWithParameters() throws Throwable
    {
        // When
        session.run( "CREATE (n:FirstNode {name:{name}})", parameters( "name", "Steven" ) );

        // Then nothing should've failed
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void shouldRunWithNullValuesAsParameters() throws Throwable
    {
        // Given
        Value params = null;

        // When
        session.run( "CREATE (n:FirstNode {name:'Steven'})", params );

        // Then nothing should've failed
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void shouldRunWithNullRecordAsParameters() throws Throwable
    {
        // Given
        Record params = null;

        // When
        session.run( "CREATE (n:FirstNode {name:'Steven'})", params );

        // Then nothing should've failed
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void shouldRunWithNullMapAsParameters() throws Throwable
    {
        // Given
        Map<String, Object> params = null;

        // When
        session.run( "CREATE (n:FirstNode {name:'Steven'})", params );

        // Then nothing should've failed
    }

    @Test
    public void shouldRunWithCollectionAsParameter() throws Throwable
    {
        // When
        session.run( "RETURN {param}", parameters( "param", Collections.singleton( "FOO" ) ) );

        // Then nothing should've failed
    }

    @Test
    public void shouldRunWithIteratorAsParameter() throws Throwable
    {
        Iterator<String> values = asList( "FOO", "BAR", "BAZ" ).iterator();
        // When
        session.run( "RETURN {param}", parameters( "param", values ) );

        // Then nothing should've failed
    }

    @Test
    public void shouldRun() throws Throwable
    {
        // When
        session.run( "CREATE (n:FirstNode)" );

        // Then nothing should've failed
    }

    @Test
    public void shouldRunParameterizedWithResult() throws Throwable
    {
        // When
        List<Record> result =
                session.run( "UNWIND {list} AS k RETURN k", parameters( "list", asList( 1, 2, 3 ) ) ).list();

        // Then
        assertThat( result.size(), equalTo( 3 ) );
    }

    @SuppressWarnings({"StatementWithEmptyBody", "ConstantConditions"})
    @Test
    public void shouldRunSimpleStatement() throws Throwable
    {
        // When I run a simple write statement
        session.run( "CREATE (a {name:'Adam'})" );

        // And I run a read statement
        StatementResult result2 = session.run( "MATCH (a) RETURN a.name" );

        // Then I expect to get the name back
        Value name = null;
        while ( result2.hasNext() )
        {
            name = result2.next().get( "a.name" );
        }

        assertThat( name.asString(), equalTo( "Adam" ) );
    }
}
