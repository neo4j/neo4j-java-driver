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
package org.neo4j.driver.types;

import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

import org.neo4j.driver.util.Experimental;
import org.neo4j.driver.util.Immutable;

/**
 * A listing of all database types this driver can handle.
 * @since 1.0
 */
@Immutable
@Experimental
public interface TypeSystem {
    /**
     * Returns an instance of type system.
     *
     * @return instance of type system
     */
    static TypeSystem getDefault() {
        return TYPE_SYSTEM;
    }

    Type ANY();

    Type BOOLEAN();

    Type BYTES();

    Type STRING();

    Type NUMBER();

    Type INTEGER();

    Type FLOAT();

    Type LIST();

    Type MAP();

    Type NODE();

    Type RELATIONSHIP();

    Type PATH();

    Type POINT();

    Type DATE();

    Type TIME();

    Type LOCAL_TIME();

    Type LOCAL_DATE_TIME();

    Type DATE_TIME();

    Type DURATION();

    Type NULL();
}
