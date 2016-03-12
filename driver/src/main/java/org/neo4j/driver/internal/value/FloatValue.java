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

public class FloatValue extends NumberValueAdapter<Double>
{
    private final double val;

    public FloatValue( double val )
    {
        this.val = val;
    }

    @Override
    public Type type()
    {
        return InternalTypeSystem.TYPE_SYSTEM.FLOAT();
    }

    @Override
    public Double asNumber()
    {
        return val;
    }

    @Override
    public long asLong()
    {
        long longVal = (long) val;
        if ((double) longVal != val)
        {
            throw new LossyCoercion( type().name(), "Java long" );
        }

        return longVal;
    }

    @Override
    public int asInt()
    {
        int intVal = (int) val;
        if ((double) intVal != val)
        {
            throw new LossyCoercion( type().name(), "Java int" );
        }

        return intVal;
    }

    @Override
    public double asDouble()
    {
        return val;
    }

    @Override
    public float asFloat()
    {
        float floatVal = (float) val;
        if ((double) floatVal != val)
        {
            throw new LossyCoercion( type().name(), "Java float" );
        }

        return floatVal;
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

        FloatValue values = (FloatValue) o;
        return Double.compare( values.val, val ) == 0;
    }

    @Override
    public int hashCode()
    {
        long temp = Double.doubleToLongBits( val );
        return (int) (temp ^ (temp >>> 32));
    }

    @Override
    public String asLiteralString()
    {
        return Double.toString( val );
    }
}
