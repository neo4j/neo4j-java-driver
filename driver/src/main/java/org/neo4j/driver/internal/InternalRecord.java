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
package org.neo4j.driver.internal;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.util.Extract;
import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;

import static org.neo4j.driver.v1.Values.valueAsIs;

public class InternalRecord extends InternalRecordAccessor implements Record
{
    private final List<String> keys;
    private final Map<String, Integer> keyIndexLookup;
    private final Value[] values;

    public InternalRecord( List<String> keys, Map<String, Integer> keyIndexLookup, Value[] values )
    {
        this.keys = keys;
        this.keyIndexLookup = keyIndexLookup;
        this.values = values;
    }

    @Override
    public Value value( int index )
    {
        return index >= 0 && index < values.length ? values[index] : Values.NULL;
    }

    @Override
    public List<String> keys()
    {
        return keys;
    }

    @Override
    public boolean containsKey( String key )
    {
        return keyIndexLookup.containsKey( key );
    }

    @Override
    public Value value( String key )
    {
        Integer fieldIndex = keyIndexLookup.get( key );

        if ( fieldIndex == null )
        {
            return Values.NULL;
        }
        else
        {
            return values[fieldIndex];
        }
    }

    @Override
    public Record record()
    {
        return this;
    }

    @Override
    public Map<String, Value> asMap()
    {
        return asMap( valueAsIs() );
    }

    public <T> Map<String, T> asMap( Function<Value, T> mapFunction )
    {
        return Extract.map( this, mapFunction );
    }

    @Override
    public int hashCode()
    {
        int result = keys.hashCode();
        result = 31 * result + Arrays.hashCode( values );
        return result;
    }
}
