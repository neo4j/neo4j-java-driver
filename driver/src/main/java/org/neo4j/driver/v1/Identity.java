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
package org.neo4j.driver.v1;

/**
 * A unique identifier for an {@link Entity}.
 * <p>
 * The identity can be used to correlate entities in one response with entities received earlier. The identity of an
 * entity is guaranteed to be stable within the scope of a session. Beyond that, the identity may change. If you want
 * stable 'ids' for your entities, you may choose to add an 'id' property with a {@link java.util.UUID} or similar
 * unique value.
 */
public interface Identity
{
    // Force implementation
    @Override
    boolean equals( Object other );

    // Force implementation
    @Override
    int hashCode();

    // NOTE: This should be removed, but is kept until the Identity CIP is played
    // Return a long representation of this identity
    long asLong();
}
