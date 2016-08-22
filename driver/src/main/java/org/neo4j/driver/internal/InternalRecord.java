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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.driver.internal.util.Extract;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.internal.types.InternalMapAccessorWithDefaultValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.Pair;

import static java.lang.String.format;
import static org.neo4j.driver.internal.util.Format.formatPairs;
import static org.neo4j.driver.v1.Values.ofObject;
import static org.neo4j.driver.v1.Values.ofValue;

public class InternalRecord extends InternalMapAccessorWithDefaultValue implements Record
{
    private final List<String> keys;
    private final Value[] values;
    private int hashcode = 0;

    public InternalRecord( List<String> keys, Value[] values )
    {
        this.keys = keys;
        this.values = values;
    }

    @Override
    public List<String> keys()
    {
        return keys;
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
        int result = keys.indexOf( key );
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
        return keys.contains( key );
    }

    @Override
    public Value get( String key )
    {
        int fieldIndex = keys.indexOf( key );

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
        return format( "Record<%s>", formatPairs( InternalValue.Format.VALUE_ONLY, asMap( ofValue() ) ) );
    }

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
            if ( ! keys.equals( otherRecord.keys() ) )
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

    public int hashcode()
    {
        if ( hashcode == 0 )
        {
            hashcode = 31 * keys.hashCode() + Arrays.hashCode( values );
        }
        return hashcode;
    }
}
