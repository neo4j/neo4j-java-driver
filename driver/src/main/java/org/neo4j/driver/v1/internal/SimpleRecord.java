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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;

public class SimpleRecord implements Record
{
    private final Map<String,Integer> fieldLookup;
    private final Value[] fields;

    public static Record record( Object... alternatingFieldNameValue )
    {
        Map<String,Integer> lookup = new HashMap<>();
        Value[] fields = new Value[alternatingFieldNameValue.length / 2];
        for ( int i = 0; i < alternatingFieldNameValue.length; i += 2 )
        {
            lookup.put( alternatingFieldNameValue[i].toString(), i / 2 );
            fields[i / 2] = (Value) alternatingFieldNameValue[i + 1];
        }
        return new SimpleRecord( lookup, fields );
    }

    public SimpleRecord( Map<String,Integer> fieldLookup, Value[] fields )
    {
        this.fieldLookup = fieldLookup;
        this.fields = fields;
    }

    @Override
    public Value get( int fieldIndex )
    {
        return fields[fieldIndex];
    }

    @Override
    public Value get( String fieldName )
    {
        Integer fieldIndex = fieldLookup.get( fieldName );

        if ( fieldIndex == null )
        {
            return null;
        }
        else
        {
            return fields[fieldIndex];
        }
    }

    @Override
    public Iterable<String> fieldNames()
    {
        return fieldLookup.keySet();
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

        SimpleRecord that = (SimpleRecord) o;

        if ( !fieldLookup.equals( that.fieldLookup ) )
        {
            return false;
        }
        if ( !Arrays.equals( fields, that.fields ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = fieldLookup.hashCode();
        result = 31 * result + Arrays.hashCode( fields );
        return result;
    }
}
