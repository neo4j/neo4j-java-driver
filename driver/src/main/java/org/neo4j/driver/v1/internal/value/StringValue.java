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

import org.neo4j.driver.v1.Type;
import org.neo4j.driver.v1.exceptions.value.Unrepresentable;
import org.neo4j.driver.v1.internal.types.StandardTypeSystem;

public class StringValue extends ValueAdapter
{
    private final String val;

    public StringValue( String val )
    {
        assert val != null;
        this.val = val;
    }

    @Override
    public boolean isEmpty()
    {
        return val.isEmpty();
    }

    @Override
    public String asString()
    {
        return val;
    }

    @Override
    public char asChar()
    {
        if ( val.length() == 1 )
        {
            return val.charAt( 0 );
        }
        else
        {
            throw new Unrepresentable( "Only a STRING of exactly one character can be represented as a Java char" );
        }
    }

    @Override
    public char[] asCharArray()
    {
        return val.toCharArray();
    }

    @Override
    public int size()
    {
        return val.length();
    }

    @Override
    public Type type()
    {
        return StandardTypeSystem.TYPE_SYSTEM.STRING();
    }

    public Object asObject()
    {
        return asString();
    }

    @SuppressWarnings("StringEquality")
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

        StringValue values = (StringValue) o;
        return val == values.val || val.equals( values.val );
    }

    @Override
    public int hashCode()
    {
        return val.hashCode();
    }

    @Override
    protected String asLiteralString()
    {
        return String.format( "\"%s\"", val.replace( "\"", "\\\"" ) );
    }
}
