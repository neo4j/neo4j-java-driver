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

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.util.TestNeo4jSession;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

public class EntityTypeIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldReturnIdentitiesOfNodes() throws Throwable
    {
        // When
        StatementResult cursor = session.run( "CREATE (n) RETURN n" );
        Node node = cursor.single().get( "n" ).asNode();

        // Then
        assertThat( node.id(), greaterThan(-1L));
    }

    @Test
    public void shouldReturnIdentitiesOfRelationships() throws Throwable
    {
        // When
        StatementResult cursor = session.run( "CREATE ()-[r:T]->() RETURN r" );
        Relationship rel = cursor.single().get( "r" ).asRelationship();

        // Then
        assertThat( rel.startNodeId(), greaterThan(-1L));
        assertThat( rel.endNodeId(), greaterThan(-1L));
        assertThat( rel.id(), greaterThan(-1L));
    }

    @Test
    public void shouldReturnIdentitiesOfPaths() throws Throwable
    {
        // When
        StatementResult cursor = session.run( "CREATE p=()-[r:T]->() RETURN p" );
        Path path = cursor.single().get( "p" ).asPath();

        // Then
        assertThat( path.start().id(), greaterThan( -1L ));
        assertThat( path.end().id(), greaterThan( -1L ));

        Path.Segment segment = path.iterator().next();

        assertThat( segment.start().id(), greaterThan( -1L ));
        assertThat( segment.relationship().id(), greaterThan( -1L ));
        assertThat( segment.end().id(), greaterThan( -1L ));
    }

}
