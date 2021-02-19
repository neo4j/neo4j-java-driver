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

import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.util.ParallelizableIT;
import org.neo4j.driver.util.SessionExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@ParallelizableIT
class EntityTypeIT
{
    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldReturnIdentitiesOfNodes()
    {
        // When
        Result cursor = session.run( "CREATE (n) RETURN n" );
        Node node = cursor.single().get( "n" ).asNode();

        // Then
        assertThat( node.id(), greaterThan(-1L));
    }

    @Test
    void shouldReturnIdentitiesOfRelationships()
    {
        // When
        Result cursor = session.run( "CREATE ()-[r:T]->() RETURN r" );
        Relationship rel = cursor.single().get( "r" ).asRelationship();

        // Then
        assertThat( rel.startNodeId(), greaterThan(-1L));
        assertThat( rel.endNodeId(), greaterThan(-1L));
        assertThat( rel.id(), greaterThan(-1L));
    }

    @Test
    void shouldReturnIdentitiesOfPaths()
    {
        // When
        Result cursor = session.run( "CREATE p=()-[r:T]->() RETURN p" );
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
