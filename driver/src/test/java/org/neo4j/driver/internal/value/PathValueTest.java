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
package org.neo4j.driver.internal.value;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.driver.internal.util.ValueFactory.filledPathValue;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Path;

class PathValueTest {
    @Test
    void shouldHaveSensibleToString() {
        assertEquals("path[(42)-[43:T]->(44)]", filledPathValue().toString());
    }

    @Test
    void shouldNotBeNull() {
        Value value = filledPathValue();
        assertFalse(value.isNull());
    }

    @Test
    void shouldHaveCorrectType() {
        assertThat(filledPathValue().type(), equalTo(InternalTypeSystem.TYPE_SYSTEM.PATH()));
    }

    @Test
    void shouldMapToType() {
        var path = new InternalPath(
                new InternalNode(42L), new InternalRelationship(43L, 42L, 44L, "T"), new InternalNode(44L));
        var values = Values.value(path);
        assertEquals(path, values.as(Path.class));
        assertEquals(path, values.as(Iterable.class));
        assertEquals(path, values.as(Object.class));
    }
}
