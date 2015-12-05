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
package org.neo4j.driver.v1.internal;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;

import static java.util.Collections.emptyMap;

public class EmptyRecord extends SimpleRecordAccessor implements Record
{
    private final List<String> keys;

    EmptyRecord( List<String> keys )
    {
        this.keys = keys;
    }

    public Value value( int index )
    {
        return Values.NULL;
    }

    @Override
    public boolean containsKey( String key )
    {
        return keys.contains( key );
    }

    public List<String> keys()
    {
        return keys;
    }

    @Override
    public Value value( String key )
    {
        return Values.NULL;
    }

    @Override
    public Map<String, Value> asMap()
    {
        return emptyMap();
    }

    @Override
    public <T> Map<String, T> asMap( Function<Value, T> mapFunction )
    {
        return emptyMap();
    }
}
