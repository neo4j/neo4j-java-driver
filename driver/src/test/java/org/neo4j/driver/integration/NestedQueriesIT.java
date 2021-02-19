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

import org.junit.jupiter.api.extension.RegisterExtension;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Session;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static org.neo4j.driver.SessionConfig.builder;

@ParallelizableIT
public class NestedQueriesIT implements NestedQueries
{
    @RegisterExtension
    static final DatabaseExtension server = new DatabaseExtension();

    @Override
    public Session newSession( AccessMode mode )
    {
        return server.driver().session( builder().withDefaultAccessMode( mode ).build() );
    }
}
