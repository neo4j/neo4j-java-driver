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
import org.neo4j.driver.v1.exceptions.value.LossyCoercion;

public class IntegerValue extends NumberValueAdapter<Long>
{
    private final long val;

    public IntegerValue( long val )
    {
        this.val = val;
    }

    @Override
    public Type type()
    {
        return InternalTypeSystem.TYPE_SYSTEM.INTEGER();
    }

    @Override
    public Long asNumber()
    {
        return val;
    }

    @Override
    public long asLong()
    {
        return val;
    }

    @Override
    public int asInt()
    {
        if (val > Integer.MAX_VALUE || val < Integer.MIN_VALUE)
        {
            throw new LossyCoercion( type().name(), "Java int" );
        }
        return (int) val;
    }

    @Override
    public double asDouble()
    {
        double doubleVal = (double) val;
        if ( (long) doubleVal != val)
        {
            throw new LossyCoercion( type().name(), "Java double" );
        }

        return (double) val;
    }

    @Override
    public float asFloat()
    {
        return (float) val;
    }

    @Override
    public String asLiteralString()
    {
        return Long.toString( val );
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

        IntegerValue values = (IntegerValue) o;
        return val == values.val;
    }

    @Override
    public int hashCode()
    {
        return (int) (val ^ (val >>> 32));
    }
}
