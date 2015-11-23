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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Function;

public class MapValue extends ValueAdapter
{
    private final Map<String,Value> val;

    public MapValue( Map<String,Value> val )
    {
        this.val = val;
    }

    @Override
    public boolean javaBoolean()
    {
        return !val.isEmpty();
    }

    @Override
    public <T> List<T> javaList( Function<Value,T> mapFunction )
    {
        List<T> list = new ArrayList<>( val.size() );
        for ( Value value : val.values() )
        {
            list.add( mapFunction.apply( value ) );
        }
        return list;
    }

    @Override
    public <T> Map<String, T> javaMap( Function<Value,T> mapFunction )
    {
        Map<String, T> map = new HashMap<>( val.size() );
        for ( Map.Entry<String, Value> entry : val.entrySet() )
        {
            map.put( entry.getKey(), mapFunction.apply( entry.getValue() ) );
        }
        return map;
    }

    @Override
    public long size()
    {
        return val.size();
    }

    @Override
    public Iterable<String> keys()
    {
        return val.keySet();
    }

    @Override
    public boolean isMap()
    {
        return true;
    }

    @Override
    public Iterator<Value> iterator()
    {
        final Iterator<Value> raw = val.values().iterator();
        return new Iterator<Value>()
        {
            @Override
            public boolean hasNext()
            {
                return raw.hasNext();
            }

            @Override
            public Value next()
            {
                return raw.next();
            }

            @Override
            public void remove()
            {
            }
        };
    }

    @Override
    public Value get( String key )
    {
        return val.get( key );
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

        return !(val != null ? !val.equals( values.val ) : values.val != null);

    }

    @Override
    public int hashCode()
    {
        return val != null ? val.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return String.format( "map<%s>", val.toString() );
    }
}
