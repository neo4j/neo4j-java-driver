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

import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

import org.neo4j.driver.util.Immutable;

/**
 * A listing of all database types this driver can handle.
 * @since 1.0
 */
@Immutable
public interface TypeSystem {
    /**
     * Returns an instance of type system.
     *
     * @return instance of type system
     */
    @SuppressWarnings("SameReturnValue")
    static TypeSystem getDefault() {
        return TYPE_SYSTEM;
    }

    /**
     * Returns a {@link Type} instance representing any type.
     * @return the type instance
     */
    Type ANY();

    /**
     * Returns a {@link Type} instance representing boolean.
     * @return the type instance
     */
    Type BOOLEAN();

    /**
     * Returns a {@link Type} instance representing bytes.
     * @return the type instance
     */
    Type BYTES();

    /**
     * Returns a {@link Type} instance representing string.
     * @return the type instance
     */
    Type STRING();

    /**
     * Returns a {@link Type} instance representing number.
     * @return the type instance
     */
    Type NUMBER();

    /**
     * Returns a {@link Type} instance representing integer.
     * @return the type instance
     */
    Type INTEGER();

    /**
     * Returns a {@link Type} instance representing float.
     * @return the type instance
     */
    Type FLOAT();

    /**
     * Returns a {@link Type} instance representing list.
     * @return the type instance
     */
    Type LIST();

    /**
     * Returns a {@link Type} instance representing map.
     * @return the type instance
     */
    Type MAP();

    /**
     * Returns a {@link Type} instance representing node.
     * @return the type instance
     */
    Type NODE();

    /**
     * Returns a {@link Type} instance representing relationship.
     * @return the type instance
     */
    Type RELATIONSHIP();

    /**
     * Returns a {@link Type} instance representing path.
     * @return the type instance
     */
    Type PATH();

    /**
     * Returns a {@link Type} instance representing point.
     * @return the type instance
     */
    Type POINT();

    /**
     * Returns a {@link Type} instance representing date.
     * @return the type instance
     */
    Type DATE();

    /**
     * Returns a {@link Type} instance representing time.
     * @return the type instance
     */
    Type TIME();

    /**
     * Returns a {@link Type} instance representing local time.
     * @return the type instance
     */
    Type LOCAL_TIME();

    /**
     * Returns a {@link Type} instance representing local date time.
     * @return the type instance
     */
    Type LOCAL_DATE_TIME();

    /**
     * Returns a {@link Type} instance representing date time.
     * @return the type instance
     */
    Type DATE_TIME();

    /**
     * Returns a {@link Type} instance representing duration.
     * @return the type instance
     */
    Type DURATION();

    /**
     * Returns a {@link Type} instance representing NULL.
     * @return the type instance
     */
    Type NULL();
}
