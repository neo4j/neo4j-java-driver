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
package org.neo4j.driver.v1.internal.value;

import java.util.Map;

import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.Type;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.internal.types.StandardTypeSystem;
import org.neo4j.driver.v1.internal.util.Extract;

public class MapValue extends ValueAdapter
{
    private final Map<String, Value> val;

    public MapValue( Map<String, Value> val )
    {
        this.val = val;
    }

    @Override
    public <T> Map<String, T> asMap( Function<Value, T> mapFunction )
    {
        return Extract.map( val, mapFunction );
    }

    @Override
    public Map<String,Value> asMap()
    {
        return Extract.map( val );
    }

    public Object asObject()
    {
        return asMap();
    }

    @Override
    public int countElements()
    {
        return val.size();
    }

    @Override
    public boolean hasKey( String key )
    {
        return val.containsKey( key );
    }

    @Override
    public Iterable<String> keys()
    {
        return val.keySet();
    }

    @Override
    public Iterable<Value> values()
    {
        return val.values();
    }

    @Override
    public Value value( String key )
    {
        return val.get( key );
    }


    @Override
    public Type type()
    {
        return StandardTypeSystem.TYPE_SYSTEM.MAP();
    }

    @Override
    public String toString()
    {
        return String.format( "map<%s>", val.toString() );
    }

    @Override
    public <T> Iterable<T> values( Function<Value, T> mapFunction )
    {
        return Extract.list( this, mapFunction );
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

        MapValue values = (MapValue) o;
        return val == values.val || val.equals( values.val );
    }

    @Override
    public int hashCode()
    {
        return val != null ? val.hashCode() : 0;
    }
}
