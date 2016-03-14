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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;

@RunWith(Parameterized.class)
public class ScalarTypeIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Parameterized.Parameter(0)
    public String statement;

    @Parameterized.Parameter(1)
    public Value expectedValue;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> typesToTest()
    {
        return Arrays.asList(
                new Object[]{"RETURN 1 as v", Values.value( 1L )},
                new Object[]{"RETURN 1.1 as v", Values.value( 1.1d )},
                new Object[]{"RETURN 'hello' as v", Values.value( "hello" )},
                new Object[]{"RETURN true as v", Values.value( true )},
                new Object[]{"RETURN false as v", Values.value( false )},
                new Object[]{"RETURN [1,2,3] as v", new ListValue( Values.value( 1 ), Values.value( 2 ), Values.value( 3 ) )},
                new Object[]{"RETURN ['hello'] as v", new ListValue( Values.value( "hello" ) )},
                new Object[]{"RETURN [] as v", new ListValue()},
                new Object[]{"RETURN {k:'hello'} as v", parameters( "k", Values.value( "hello" ) )},
                new Object[]{"RETURN {} as v", new MapValue( Collections.<String, Value>emptyMap() )}
        );
    }

    @Test
    public void shouldHandleType() throws Throwable
    {
        // When
        StatementResult cursor = session.run( statement );

        // Then
        assertThat( cursor.single().get( "v" ), equalTo( expectedValue ) );
    }
}
