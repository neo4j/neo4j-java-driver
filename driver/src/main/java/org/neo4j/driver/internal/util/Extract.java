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
package org.neo4j.driver.internal.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.InternalPair;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.MapAccessor;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.Pair;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Utility class for extracting data.
 */
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

    public static <T> List<T> list( Value[] data, Function<Value, T> mapFunction )
    {
        int size = data.length;
        switch ( size )
        {
            case 0:
                return emptyList();
            case 1:
                return singletonList( mapFunction.apply( data[0] ) );
            default:
                List<T> result = new ArrayList<>( size );
                for ( Value value : data )
                {
                    result.add( mapFunction.apply( value ) );
                }
                return unmodifiableList( result );
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
        if ( data.isEmpty() ) {
            return emptyMap();
        } else {
            int size = data.size();
            if ( size == 1 ) {
                Map.Entry<String, Value> head = data.entrySet().iterator().next();
                return singletonMap( head.getKey(), mapFunction.apply( head.getValue() ) );
            } else {
                Map<String, T> map = new LinkedHashMap<>( size );
                for ( Map.Entry<String, Value> entry : data.entrySet() )
                {
                    map.put( entry.getKey(), mapFunction.apply( entry.getValue() ) );
                }
                return unmodifiableMap( map );
            }
        }
    }

    public static <T> Map<String, T> map( Record record, Function<Value, T> mapFunction )
    {
        int size = record.size();
        switch ( size )
        {
            case 0:
                return emptyMap();

            case 1:
                return singletonMap( record.keys().get( 0 ), mapFunction.apply( record.get( 0 ) ) );

            default:
                Map<String, T> map = new LinkedHashMap<>( size );
                List<String> keys = record.keys();
                for ( int i = 0; i < size; i++ )
                {
                    map.put( keys.get( i ), mapFunction.apply( record.get( i ) ) );
                }
                return unmodifiableMap( map );
        }
    }

    public static <V> Iterable<Pair<String, V>> properties( final MapAccessor map, final Function<Value, V> mapFunction )
    {
        int size = map.size();
        switch ( size )
        {
            case 0:
                return emptyList();

            case 1:
            {
                String key = map.keys().iterator().next();
                Value value = map.get( key );
                return singletonList( InternalPair.of( key, mapFunction.apply( value ) ) );
            }

            default:
            {
                List<Pair<String, V>> list = new ArrayList<>( size );
                for ( String key : map.keys() )
                {
                    Value value = map.get( key );
                    list.add( InternalPair.of( key, mapFunction.apply( value ) ) );
                }
                return unmodifiableList( list );
            }
        }
    }

    public static <V> List<Pair<String, V>> fields( final Record map, final Function<Value, V> mapFunction )
    {
        int size = map.keys().size();
        switch ( size )
        {
            case 0:
                return emptyList();

            case 1:
            {
                String key = map.keys().iterator().next();
                Value value = map.get( key );
                return singletonList( InternalPair.of( key, mapFunction.apply( value ) ) );
            }

            default:
            {
                List<Pair<String, V>> list = new ArrayList<>( size );
                List<String> keys = map.keys();
                for ( int i = 0; i < size; i++ )
                {
                    String key = keys.get( i );
                    Value value = map.get( i );
                    list.add( InternalPair.of( key, mapFunction.apply( value ) ) );
                }
                return unmodifiableList( list );
            }
        }
    }
}
