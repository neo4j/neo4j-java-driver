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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.util.TestSession;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.Neo4j.parameters;
import static org.neo4j.driver.Values.value;

@RunWith(Parameterized.class)
public class ScalarTypeIT
{
    @Rule
    public TestSession session = new TestSession();

    @Parameterized.Parameter(0)
    public String statement;

    @Parameterized.Parameter(1)
    public Value expectedValue;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> typesToTest()
    {
        return Arrays.asList(
                new Object[]{"RETURN 1 as v", value( 1l )},
                new Object[]{"RETURN 1.1 as v", value( 1.1d )},
                new Object[]{"RETURN 'hello' as v", value( "hello" )},
                new Object[]{"RETURN true as v", value( true )},
                new Object[]{"RETURN false as v", value( false )},
                new Object[]{"RETURN [1,2,3] as v", new ListValue( value( 1 ), value( 2 ), value( 3 ) )},
                new Object[]{"RETURN ['hello'] as v", new ListValue( value( "hello" ) )},
                new Object[]{"RETURN [] as v", new ListValue()},
                new Object[]{"RETURN {k:'hello'} as v", new MapValue( parameters( "k", value( "hello" ) ) )},
                new Object[]{"RETURN {} as v", new MapValue( Collections.EMPTY_MAP )}
        );
    }

    @Test
    public void shouldHandleType() throws Throwable
    {
        // When
        Value value = session.run( statement ).single().get( "v" );

        // Then
        assertThat( value, equalTo( expectedValue ) );
    }
}
