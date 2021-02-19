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
package org.neo4j.driver.internal;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.types.InternalMapAccessorWithDefaultValue;
import org.neo4j.driver.internal.util.Extract;
import org.neo4j.driver.internal.util.QueryKeys;
import org.neo4j.driver.util.Pair;

import static java.lang.String.format;
import static org.neo4j.driver.Values.ofObject;
import static org.neo4j.driver.Values.ofValue;
import static org.neo4j.driver.internal.util.Format.formatPairs;

public class InternalRecord extends InternalMapAccessorWithDefaultValue implements Record
{
    private final QueryKeys queryKeys;
    private final Value[] values;
    private int hashCode = 0;

    public InternalRecord( List<String> keys, Value[] values )
    {
        this.queryKeys = new QueryKeys( keys );
        this.values = values;
    }

    public InternalRecord( QueryKeys queryKeys, Value[] values )
    {
        this.queryKeys = queryKeys;
        this.values = values;
    }

    @Override
    public List<String> keys()
    {
        return queryKeys.keys();
    }

    @Override
    public List<Value> values()
    {
        return Arrays.asList( values );
    }

    @Override
    public List<Pair<String, Value>> fields()
    {
        return Extract.fields( this, ofValue() );
    }

    @Override
    public int index( String key )
    {
        int result = queryKeys.indexOf( key );
        if ( result == -1 )
        {
            throw new NoSuchElementException( "Unknown key: " + key );
        }
        else
        {
            return result;
        }
    }

    @Override
    public boolean containsKey( String key )
    {
        return queryKeys.contains( key );
    }

    @Override
    public Value get( String key )
    {
        int fieldIndex = queryKeys.indexOf( key );

        if ( fieldIndex == -1 )
        {
            return Values.NULL;
        }
        else
        {
            return values[fieldIndex];
        }
    }

    @Override
    public Value get( int index )
    {
        return index >= 0 && index < values.length ? values[index] : Values.NULL;
    }

    @Override
    public int size()
    {
        return values.length;
    }

    @Override
    public Map<String, Object> asMap()
    {
        return Extract.map( this, ofObject() );
    }

    @Override
    public <T> Map<String,T> asMap( Function<Value,T> mapper )
    {
        return Extract.map( this, mapper );
    }

    @Override
    public String toString()
    {
        return format( "Record<%s>", formatPairs( asMap( ofValue() ) ) );
    }

    @Override
    public boolean equals( Object other )
    {
        if ( this == other )
        {
            return true;
        }
        else if ( other instanceof Record )
        {
            Record otherRecord = (Record) other;
            int size = size();
            if ( ! ( size == otherRecord.size() ) )
            {
                return false;
            }
            if ( !queryKeys.keys().equals( otherRecord.keys() ) )
            {
                return false;
            }
            for ( int i = 0; i < size; i++ )
            {
                Value value = get( i );
                Value otherValue = otherRecord.get( i );
                if ( ! value.equals( otherValue ) )
                {
                    return false;
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        if ( hashCode == 0 )
        {
            hashCode = 31 * queryKeys.hashCode() + Arrays.hashCode( values );
        }
        return hashCode;
    }
}
