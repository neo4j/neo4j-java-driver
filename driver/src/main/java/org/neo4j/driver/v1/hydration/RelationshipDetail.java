/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

package org.neo4j.driver.v1.hydration;

import org.neo4j.driver.v1.Value;

import java.util.Map;

class RelationshipDetail
{
    private final long id;
    private final String type;
    private final Map<String, Value> properties;

    RelationshipDetail(long id, String type, Map<String, Value> properties)
    {
        this.id = id;
        this.type = type;
        this.properties = properties;
    }

    long id()
    {
        return id;
    }

    String type()
    {
        return type;
    }

    Map<String, Value> properties()
    {
        return properties;
    }
}
