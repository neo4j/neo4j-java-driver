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
package org.neo4j.driver.types;

/**
 * The <strong>Relationship</strong> interface describes the characteristics of a relationship from a Neo4j graph.
 * @since 1.0
 */
public interface Relationship extends Entity {
    /**
     * The id of the node where this relationship starts.
     * <p>
     * Please note that depending on server configuration numeric id might not be available and accessing it will result in {@link IllegalStateException}.
     *
     * @return the node id
     * @deprecated superseded by {@link #startNodeElementId()}
     */
    @Deprecated
    @SuppressWarnings("DeprecatedIsStillUsed")
    long startNodeId();

    /**
     * The id of the node where this relationship starts.
     *
     * @return the node id
     */
    String startNodeElementId();

    /**
     * The id of the node where this relationship ends.
     * <p>
     * Please note that depending on server configuration numeric id might not be available and accessing it will result in {@link IllegalStateException}.
     *
     * @return the node id
     * @deprecated superseded by {@link #endNodeElementId()}
     */
    @Deprecated
    @SuppressWarnings("DeprecatedIsStillUsed")
    long endNodeId();

    /**
     * The id of the node where this relationship ends.
     *
     * @return the node id
     */
    String endNodeElementId();

    /**
     * Return the <em>type</em> of this relationship.
     *
     * @return the type name
     */
    String type();

    /**
     * Test if this relationship has the given type
     *
     * @param relationshipType the give relationship type
     * @return {@code true} if this relationship has the given relationship type otherwise {@code false}
     */
    boolean hasType(String relationshipType);
}
