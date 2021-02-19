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

import org.neo4j.driver.util.Immutable;

/**
 * A uniquely identifiable property container that can form part of a Neo4j graph.
 * @since 1.0
 */
@Immutable
public interface Entity extends MapAccessor
{
    /**
     * A unique id for this Entity. Ids are guaranteed to remain stable for the duration of the session they
     * were found in, but may be re-used for other entities after that. As such, if you want a public identity to use
     * for your entities, attaching an explicit 'id' property or similar persistent and unique identifier is a better
     * choice.
     *
     * @return the id of this entity
     */
    long id();
}
