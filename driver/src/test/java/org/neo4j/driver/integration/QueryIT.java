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
package org.neo4j.driver.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.util.ParallelizableIT;
import org.neo4j.driver.util.SessionExtension;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Values.parameters;

@ParallelizableIT
class QueryIT
{
    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldRunWithResult()
    {
        // When I execute a query that yields a result
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
    void shouldRunWithParameters()
    {
        // When
        session.run( "CREATE (n:FirstNode {name:$name})", parameters( "name", "Steven" ) );

        // Then nothing should've failed
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    void shouldRunWithNullValuesAsParameters()
    {
        // Given
        Value params = null;

        // When
        session.run( "CREATE (n:FirstNode {name:'Steven'})", params );

        // Then nothing should've failed
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    void shouldRunWithNullRecordAsParameters()
    {
        // Given
        Record params = null;

        // When
        session.run( "CREATE (n:FirstNode {name:'Steven'})", params );

        // Then nothing should've failed
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    void shouldRunWithNullMapAsParameters()
    {
        // Given
        Map<String, Object> params = null;

        // When
        session.run( "CREATE (n:FirstNode {name:'Steven'})", params );

        // Then nothing should've failed
    }

    @Test
    void shouldRunWithCollectionAsParameter()
    {
        // When
        session.run( "RETURN $param", parameters( "param", Collections.singleton( "FOO" ) ) );

        // Then nothing should've failed
    }

    @Test
    void shouldRunWithIteratorAsParameter()
    {
        Iterator<String> values = asList( "FOO", "BAR", "BAZ" ).iterator();
        // When
        session.run( "RETURN $param", parameters( "param", values ) );

        // Then nothing should've failed
    }

    @Test
    void shouldRun()
    {
        // When
        session.run( "CREATE (n:FirstNode)" );

        // Then nothing should've failed
    }

    @Test
    void shouldRunParameterizedWithResult()
    {
        // When
        List<Record> result =
                session.run( "UNWIND $list AS k RETURN k", parameters( "list", asList( 1, 2, 3 ) ) ).list();

        // Then
        assertThat( result.size(), equalTo( 3 ) );
    }

    @SuppressWarnings({"QueryWithEmptyBody", "ConstantConditions"})
    @Test
    void shouldRunSimpleQuery()
    {
        // When I run a simple write query
        session.run( "CREATE (a {name:'Adam'})" );

        // And I run a read query
        Result result2 = session.run( "MATCH (a) RETURN a.name" );

        // Then I expect to get the name back
        Value name = null;
        while ( result2.hasNext() )
        {
            name = result2.next().get( "a.name" );
        }

        assertThat( name.asString(), equalTo( "Adam" ) );
    }

    @Test
    void shouldFailForIllegalQueries()
    {
        assertThrows( IllegalArgumentException.class, () -> session.run( (String) null ) );
        assertThrows( IllegalArgumentException.class, () -> session.run( "" ) );
    }
}
