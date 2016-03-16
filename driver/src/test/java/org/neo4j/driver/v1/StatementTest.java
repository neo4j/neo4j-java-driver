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
package org.neo4j.driver.v1;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;

public class StatementTest
{
    @Test
    public void shouldConstructStatementWithParameters()
    {
        // given
        String text = "MATCH (n) RETURN n";

        // when
        Statement statement = new Statement( text, Values.EmptyMap );

        // then
        assertThat( statement.text(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( Values.EmptyMap ) );
    }

    @Test
    public void shouldConstructStatementWithNoParameters()
    {
        // given
        String text = "MATCH (n) RETURN n";

        // when
        Statement statement = new Statement( text );

        // then
        assertThat( statement.text(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( Values.EmptyMap ) );
    }

    @Test
    public void shouldUpdateStatementText()
    {
        // when
        Statement statement =
                new Statement( "MATCH (n) RETURN n" )
                .withText( "BOO" );

        // then
        assertThat( statement.text(), equalTo( "BOO" ) );
        assertThat( statement.parameters(), equalTo( Values.EmptyMap ) );
    }


    @Test
    public void shouldReplaceStatementParameters()
    {
        // when
        String text = "MATCH (n) RETURN n";
        Value initialParameters = parameters( "a", 1, "b", 2 );
        Statement statement = new Statement( "MATCH (n) RETURN n" ).withParameters( initialParameters );

        // then
        assertThat( statement.text(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( initialParameters ) );
    }

    @Test
    public void shouldReplaceMapParameters()
    {
        // when
        String text = "MATCH (n) RETURN n";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put( "a", 1 );
        Statement statement = new Statement( "MATCH (n) RETURN n" ).withParameters( parameters );

        // then
        assertThat( statement.text(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( Values.value( parameters ) ) );
    }

    @Test
    public void shouldUpdateStatementParameters()
    {
        // when
        String text = "MATCH (n) RETURN n";
        Value initialParameters = parameters( "a", 1, "b", 2, "c", 3 );
        Statement statement =
                new Statement( "MATCH (n) RETURN n", initialParameters )
                .withUpdatedParameters( parameters( "a", 0, "b", Values.NULL ) );

        // then
        assertThat( statement.text(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( parameters( "a", 0, "c", 3 ) ) );
    }
}
