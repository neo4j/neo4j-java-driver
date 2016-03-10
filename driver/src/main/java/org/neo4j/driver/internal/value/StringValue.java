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
package org.neo4j.driver.internal.value;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.types.Type;

public class StringValue extends ScalarValueAdapter
{
    private final String val;

    public StringValue( String val )
    {
        if ( val == null )
        {
            throw new IllegalArgumentException( "Cannot construct StringValue from null" );
        }
        this.val = val;
    }

    @Override
    public boolean isEmpty()
    {
        return val.isEmpty();
    }

    @Override
    public int size()
    {
        return val.length();
    }

    @Override
    public String asObject()
    {
        return asString();
    }

    @Override
    public String asString()
    {
        return val;
    }

    @Override
    public String asLiteralString()
    {
        return String.format( "\"%s\"", val.replace( "\"", "\\\"" ) );
    }

    @Override
    public Type type()
    {
        return InternalTypeSystem.TYPE_SYSTEM.STRING();
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
}
