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
package org.neo4j.driver.internal.util;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.neo4j.driver.Values.value;

import java.util.HashMap;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;

public class ValueFactory {
    public static NodeValue emptyNodeValue() {
        return new NodeValue(new InternalNode(1234, singletonList("User"), new HashMap<>()));
    }

    public static NodeValue filledNodeValue() {
        return new NodeValue(new InternalNode(1234, singletonList("User"), singletonMap("name", value("Dodo"))));
    }

    public static RelationshipValue emptyRelationshipValue() {
        return new RelationshipValue(new InternalRelationship(1234, 1, 2, "KNOWS"));
    }

    public static RelationshipValue filledRelationshipValue() {
        return new RelationshipValue(
                new InternalRelationship(1234, 1, 2, "KNOWS", singletonMap("name", value("Dodo"))));
    }

    public static PathValue filledPathValue() {
        return new PathValue(new InternalPath(
                new InternalNode(42L), new InternalRelationship(43L, 42L, 44L, "T"), new InternalNode(44L)));
    }

    public static PathValue emptyPathValue() {
        return new PathValue(new InternalPath(new InternalNode(1)));
    }
}
