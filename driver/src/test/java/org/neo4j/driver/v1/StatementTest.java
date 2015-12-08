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
package org.neo4j.driver.v1;

import java.util.Map;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import static org.neo4j.driver.internal.ParameterSupport.NO_PARAMETERS;
import static org.neo4j.driver.v1.Values.parameters;

public class StatementTest
{
    @Test
    public void shouldConstructStatementWithParameters()
    {
        // given
        String text = "MATCH (n) RETURN n";

        // when
        Statement statement = new Statement( text, NO_PARAMETERS );

        // then
        assertThat( statement.template(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( NO_PARAMETERS ) );
    }

    @Test
    public void shouldConstructStatementWithNoParameters()
    {
        // given
        String text = "MATCH (n) RETURN n";

        // when
        Statement statement = new Statement( text );

        // then
        assertThat( statement.template(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( NO_PARAMETERS ) );
    }

    @Test
    public void shouldConstructStatementWithNullParameters()
    {
        // given
        String text = "MATCH (n) RETURN n";

        // when
        Statement statement = new Statement( text, null );

        // then
        assertThat( statement.template(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( NO_PARAMETERS ) );
    }


    @Test
    public void shouldUpdateStatementText()
    {
        // when
        Statement statement =
                new Statement( "MATCH (n) RETURN n" )
                .withTemplate( "BOO" );

        // then
        assertThat( statement.template(), equalTo( "BOO" ) );
        assertThat( statement.parameters(), equalTo( NO_PARAMETERS ) );
    }


    @Test
    public void shouldReplaceStatementParameters()
    {
        // when
        String text = "MATCH (n) RETURN n";
        Map<String, Value> initialParameters = parameters( "a", 1, "b", 2 );
        Statement statement = new Statement( "MATCH (n) RETURN n" ).withParameters( initialParameters );

        // then
        assertThat( statement.template(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( initialParameters ) );
    }


    @Test
    public void shouldUpdateStatementParameters()
    {
        // when
        String text = "MATCH (n) RETURN n";
        Map<String, Value> initialParameters = parameters( "a", 1, "b", 2, "c", 3 );
        Statement statement =
                new Statement( "MATCH (n) RETURN n", initialParameters )
                .withUpdatedParameters( parameters( "a", 0, "b", Values.NULL ) );

        // then
        assertThat( statement.template(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( parameters( "a", 0, "c", 3 ) ) );
    }
}
