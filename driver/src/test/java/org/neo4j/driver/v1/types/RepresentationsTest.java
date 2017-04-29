/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.neo4j.driver.v1.types;

import org.junit.Test;

import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import static org.neo4j.driver.v1.Values.value;
import static org.neo4j.driver.v1.types.Representations.repr;

public class RepresentationsTest
{
    @Test
    public void testNodeRepresentation()
    {
        // Given
        Node node = new InternalNode( 123, singletonList( "Person" ), singletonMap( "name", value( "Alice" ) ) );

        // Then
        assertThat( repr( node ), equalTo( "(_123:Person {name:'Alice'})" ) );
    }

    @Test
    public void testRelationshipRepresentation()
    {
        // Given
        Relationship rel = new InternalRelationship( 123, 45, 67, "KNOWS", singletonMap( "since", value( 1999 ) ) );

        // Then
        assertThat( repr( rel ), equalTo( "(_45)-[_123:KNOWS {since:1999}]->(_67)" ) );
    }

    @Test
    public void testPathRepresentation()
    {
        // Given
        Node a = new InternalNode( 1, singletonList( "Person" ), singletonMap( "name", value( "Alice" ) ) );
        Node b = new InternalNode( 2, singletonList( "Person" ), singletonMap( "name", value( "Bob" ) ) );
        Node c = new InternalNode( 3, singletonList( "Person" ), singletonMap( "name", value( "Carol" ) ) );
        Relationship ab = new InternalRelationship( 888, 1, 2, "KNOWS", singletonMap( "since", value( 1999 ) ) );
        Relationship cb = new InternalRelationship( 999, 3, 2, "LIKES" );
        Path path = new InternalPath( a, ab, b, cb, c );

        // Then
        assertThat( repr( path ), equalTo( "(_1)-[_888:KNOWS {since:1999}]->(_2)<-[_999:LIKES]-(_3)" ) );
    }

}