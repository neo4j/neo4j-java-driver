/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.neo4j.driver.testutil.SessionExtension;

@ParallelizableIT
class EntityTypeIT {
    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    @SuppressWarnings("deprecation")
    void shouldReturnIdentitiesOfNodes() {
        // When
        var cursor = session.run("CREATE (n) RETURN n");
        var node = cursor.single().get("n").asNode();

        // Then
        assertThat(node.id(), greaterThan(-1L));
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldReturnIdentitiesOfRelationships() {
        // When
        var cursor = session.run("CREATE ()-[r:T]->() RETURN r");
        var rel = cursor.single().get("r").asRelationship();

        // Then
        assertThat(rel.startNodeId(), greaterThan(-1L));
        assertThat(rel.endNodeId(), greaterThan(-1L));
        assertThat(rel.id(), greaterThan(-1L));
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldReturnIdentitiesOfPaths() {
        // When
        var cursor = session.run("CREATE p=()-[r:T]->() RETURN p");
        var path = cursor.single().get("p").asPath();

        // Then
        assertThat(path.start().id(), greaterThan(-1L));
        assertThat(path.end().id(), greaterThan(-1L));

        var segment = path.iterator().next();

        assertThat(segment.start().id(), greaterThan(-1L));
        assertThat(segment.relationship().id(), greaterThan(-1L));
        assertThat(segment.end().id(), greaterThan(-1L));
    }
}
