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
package org.neo4j.driver.v1.integration;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.v1.Node;
import org.neo4j.driver.v1.Path;
import org.neo4j.driver.v1.Relationship;
import org.neo4j.driver.v1.Result;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.junit.Assert.assertTrue;

public class EntityTypeIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldReturnIdentitiesOfNodes() throws Throwable
    {
        // When
        Result cursor = session.run( "CREATE (n) RETURN n" );
        assertTrue( cursor.single() );
        Node node = cursor.value( "n" ).asNode();

        // Then
        assertTrue( node.identity().toString(), node.identity().toString().matches( "#\\d+" ) );
    }

    @Test
    public void shouldReturnIdentitiesOfRelationships() throws Throwable
    {
        // When
        Result cursor = session.run( "CREATE ()-[r:T]->() RETURN r" );
        assertTrue( cursor.single() );
        Relationship rel = cursor.value( "r" ).asRelationship();

        // Then
        assertTrue( rel.start().toString(), rel.start().toString().matches( "#\\d+" ) );
        assertTrue( rel.end().toString(), rel.end().toString().matches( "#\\d+" ) );
        assertTrue( rel.identity().toString(), rel.identity().toString().matches( "#\\d+" ) );
    }

    @Test
    public void shouldReturnIdentitiesOfPaths() throws Throwable
    {
        // When
        Result cursor = session.run( "CREATE p=()-[r:T]->() RETURN p" );
        assertTrue( cursor.single() );
        Path path = cursor.value( "p" ).asPath();

        // Then
        assertTrue( path.start().identity().toString(), path.start().identity().toString().matches( "#\\d+" ) );
        assertTrue( path.end().identity().toString(), path.end().identity().toString().matches( "#\\d+" ) );

        Path.Segment segment = path.iterator().next();

        assertTrue( segment.start().identity().toString(),
                segment.start().identity().toString().matches( "#\\d+" ) );
        assertTrue( segment.relationship().identity().toString(),
                segment.relationship().identity().toString().matches( "#\\d+" ) );
        assertTrue( segment.end().identity().toString(), segment.end().identity().toString().matches( "#\\d+" ) );
    }

}
