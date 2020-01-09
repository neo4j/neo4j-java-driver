/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.driver.internal.cluster;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoutingContextTest
{
    @Test
    void emptyContextIsNotDefined()
    {
        assertFalse( RoutingContext.EMPTY.isDefined() );
    }

    @Test
    void emptyContextInEmptyMap()
    {
        assertTrue( RoutingContext.EMPTY.asMap().isEmpty() );
    }

    @Test
    void uriWithoutQueryIsParsedToEmptyContext()
    {
        testEmptyRoutingContext( URI.create( "neo4j://localhost:7687/" ) );
    }

    @Test
    void uriWithEmptyQueryIsParsedToEmptyContext()
    {
        testEmptyRoutingContext( URI.create( "neo4j://localhost:7687?" ) );
        testEmptyRoutingContext( URI.create( "neo4j://localhost:7687/?" ) );
    }

    @Test
    void uriWithQueryIsParsed()
    {
        URI uri = URI.create( "neo4j://localhost:7687/?key1=value1&key2=value2&key3=value3" );
        RoutingContext context = new RoutingContext( uri );

        assertTrue( context.isDefined() );
        Map<String,String> expectedMap = new HashMap<>();
        expectedMap.put( "key1", "value1" );
        expectedMap.put( "key2", "value2" );
        expectedMap.put( "key3", "value3" );
        assertEquals( expectedMap, context.asMap() );
    }

    @Test
    void throwsForInvalidUriQuery()
    {
        testIllegalUri( URI.create( "neo4j://localhost:7687/?justKey" ) );
    }

    @Test
    void throwsForInvalidUriQueryKey()
    {
        testIllegalUri( URI.create( "neo4j://localhost:7687/?=value1&key2=value2" ) );
    }

    @Test
    void throwsForInvalidUriQueryValue()
    {
        testIllegalUri( URI.create( "neo4j://localhost:7687/key1?=value1&key2=" ) );
    }

    @Test
    void throwsForDuplicatedUriQueryParameters()
    {
        testIllegalUri( URI.create( "neo4j://localhost:7687/?key1=value1&key2=value2&key1=value2" ) );
    }

    @Test
    void mapRepresentationIsUnmodifiable()
    {
        URI uri = URI.create( "neo4j://localhost:7687/?key1=value1" );
        RoutingContext context = new RoutingContext( uri );

        assertEquals( singletonMap( "key1", "value1" ), context.asMap() );

        assertThrows( UnsupportedOperationException.class, () -> context.asMap().put( "key2", "value2" ) );
        assertEquals( singletonMap( "key1", "value1" ), context.asMap() );
    }

    private static void testIllegalUri( URI uri )
    {
        assertThrows( IllegalArgumentException.class, () -> new RoutingContext( uri ) );
    }

    private static void testEmptyRoutingContext( URI uri )
    {
        RoutingContext context = new RoutingContext( uri );

        assertFalse( context.isDefined() );
        assertTrue( context.asMap().isEmpty() );
    }
}
