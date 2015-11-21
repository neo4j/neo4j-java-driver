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
package org.neo4j.driver.v1.internal.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.ListLike;
import org.neo4j.driver.v1.MapLike;
import org.neo4j.driver.v1.RecordLike;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.internal.SimpleMapEntry;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

public final class Extract
{
    private Extract()
    {
        throw new UnsupportedOperationException();
    }

    public static List<Value> list( Value[] values )
    {
        switch ( values.length )
        {
            case 0:
                return emptyList();
            case 1:
                return singletonList( values[0] );
            default:
                return unmodifiableList( Arrays.asList( values ) );
        }
    }

    public static <T> List<T> list( ListLike data, Function<Value, T> mapFunction )
    {
        int size = data.elementCount();
        switch ( size )
        {
            case 0:
                return emptyList();
            case 1:
                return singletonList( mapFunction.apply( data.value( 0 ) ) );
            default:
                List<T> list = new ArrayList<>( size );
                for ( Value value : data.values() )
                {
                    list.add( mapFunction.apply( value ) );
                }
                return unmodifiableList( list );
        }
    }

    public static Map<String, Value> map( Map<String, Value> data )
    {
        if ( data.isEmpty() )
        {
            return emptyMap();
        }
        else
        {
            return unmodifiableMap( data );
        }
    }

    public static <T> Map<String, T> map( Map<String, Value> data, Function<Value, T> mapFunction )
    {
        int size = data.size();
        switch ( size )
        {
            case 0:
                return emptyMap();
            case 1:
                Map.Entry<String, Value> head = data.entrySet().iterator().next();
                return singletonMap( head.getKey(), mapFunction.apply( head.getValue() ) );
            default:
                Map<String, T> map = new HashMap<>( size );
                for ( Map.Entry<String, Value> entry : data.entrySet() )
                {
                    map.put( entry.getKey(), mapFunction.apply( entry.getValue() ) );
                }
                return unmodifiableMap( map );
        }
    }

    public static Map<String, Value> map( RecordLike record )
    {
        int size = record.elementCount();
        switch ( size )
        {
            case 0:
                return emptyMap();

            case 1:
                return singletonMap( record.keys().get( 0 ), record.value( 0 ) );

            default:
                Map<String, Value> map = new HashMap<>( size );
                List<String> keys = record.keys();
                for ( int i = 0; i < size; i++ )
                {
                    map.put( keys.get( i ), record.value( i ) );
                }
                return unmodifiableMap( map );
        }
    }

    public static <V> Iterable<MapLike.Entry<V>> entries( final MapLike map, final Function<Value, V> mapFunction )
    {
        return new Iterable<MapLike.Entry<V>>()
        {
            @Override
            public Iterator<MapLike.Entry<V>> iterator()
            {
                final Iterator<String> keys = map.keys().iterator();
                return new Iterator<MapLike.Entry<V>>()
                {
                    @Override
                    public boolean hasNext()
                    {
                        return keys.hasNext();
                    }

                    @Override
                    public MapLike.Entry<V> next()
                    {
                        String key = keys.next();
                        Value value = map.value( key );
                        return SimpleMapEntry.of( key, mapFunction.apply( value ) );
                    }
                };
            }
        };
    }

    public static <V> List<MapLike.Entry<V>> entriesList( final RecordLike map, final Function<Value, V> mapFunction )
    {
        int size = map.elementCount();
        switch ( size )
        {
            case 0:
                return emptyList();

            case 1:
            {
                String key = map.keys().iterator().next();
                Value value = map.value( key );
                return singletonList( SimpleMapEntry.of( key, mapFunction.apply( value ) ) );
            }

            default:
            {
                List<MapLike.Entry<V>> list = new ArrayList<>( size );
                List<String> keys = map.keys();
                for ( int i = 0; i < size; i++ )
                {
                    String key = keys.get( i );
                    Value value = map.value( i );
                    list.add( SimpleMapEntry.of( key, mapFunction.apply( value ) ) );
                }
                return unmodifiableList( list );
            }
        }
    }
}
