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
package org.neo4j.driver.internal.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class QueryKeys
{
    private static final QueryKeys EMPTY = new QueryKeys( emptyList(), emptyMap() );

    private final List<String> keys;
    private final Map<String,Integer> keyIndex;

    public QueryKeys( int size )
    {
        this( new ArrayList<>( size ), new HashMap<>( size ) );
    }

    public QueryKeys( List<String> keys )
    {
        this.keys = keys;
        Map<String,Integer> keyIndex = new HashMap<>( keys.size() );
        int i = 0;
        for ( String key : keys )
        {
            keyIndex.put( key, i++ );
        }
        this.keyIndex = keyIndex;
    }

    public QueryKeys( List<String> keys, Map<String,Integer> keyIndex )
    {
        this.keys = keys;
        this.keyIndex = keyIndex;
    }

    public void add( String key )
    {
        int index = keys.size();
        keys.add( key );
        keyIndex.put( key, index );
    }

    public List<String> keys()
    {
        return keys;
    }

    public Map<String,Integer> keyIndex()
    {
        return keyIndex;
    }

    public static QueryKeys empty()
    {
        return EMPTY;
    }

    public int indexOf( String key )
    {
        return keyIndex.getOrDefault( key, -1 );
    }

    public boolean contains( String key )
    {
        return keyIndex.containsKey( key );
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
        QueryKeys queryKeys = (QueryKeys) o;
        return keys.equals( queryKeys.keys ) && keyIndex.equals( queryKeys.keyIndex );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( keys, keyIndex );
    }
}
