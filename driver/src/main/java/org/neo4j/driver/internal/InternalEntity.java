/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.internal;

import java.util.Map;

import org.neo4j.driver.internal.util.Extract;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.util.Function;

import static org.neo4j.driver.v1.Values.ofObject;

public abstract class InternalEntity implements Entity, AsValue
{
    private final long id;
    private final Map<String,Value> properties;

    public InternalEntity( long id, Map<String, Value> properties )
    {
        this.id = id;
        this.properties = properties;
    }

    @Override
    public long id()
    {
        return id;
    }

    @Override
    public int size()
    {
        return properties.size();
    }

    @Override
    public Map<String,Object> asMap()
    {
        return asMap( ofObject() );
    }

    @Override
    public <T> Map<String,T> asMap( Function<Value,T> mapFunction )
    {
        return Extract.map( properties, mapFunction );
    }

    @Override
    public Value asValue()
    {
        return new MapValue( properties );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        InternalEntity that = (InternalEntity) o;

        return id == that.id;

    }

    @Override
    public int hashCode()
    {
        return (int)(id ^ (id >>> 32));
    }

    @Override
    public String toString()
    {
        return "Entity{" +
               "id=" + id +
               ", properties=" + properties +
               '}';
    }

    @Override
    public boolean containsKey( String key )
    {
        return properties.containsKey( key );
    }

    @Override
    public Iterable<String> keys()
    {
        return properties.keySet();
    }

    @Override
    public Value get( String key )
    {
        Value value = properties.get( key );
        return value == null ? Values.NULL : value;
    }

    @Override
    public Iterable<Value> values()
    {
        return properties.values();
    }

    @Override
    public <T> Iterable<T> values( Function<Value,T> mapFunction )
    {
        return Iterables.map( properties.values(), mapFunction );
    }
}
