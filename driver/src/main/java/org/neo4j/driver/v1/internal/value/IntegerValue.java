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
import org.neo4j.driver.v1.internal.types.StandardTypeSystem;
import org.neo4j.driver.v1.internal.types.TypeConstructor;

public class IntegerValue extends NumberValueAdapter
{
    private final long val;

    public IntegerValue( long val )
    {
        this.val = val;
    }

    @Override
    public Number asNumber()
    {
        return asInt();
    }

    @Override
    public long asLong()
    {
        return val;
    }

    @Override
    public int asInt()
    {
        return (int) val;
    }

    @Override
    public short asShort()
    {
        return (short) val;
    }

    public byte asByte()
    {
        return (byte) val;
    }

    @Override
    public double asDouble()
    {
        return (double) val;
    }

    @Override
    public float asFloat()
    {
        return (float) val;
    }

    @Override
    public boolean isInteger()
    {
        return true;
    }

    @Override
    public Type type()
    {
        return StandardTypeSystem.TYPE_SYSTEM.INTEGER();
    }

    @Override
    public TypeConstructor typeConstructor()
    {
        return TypeConstructor.INTEGER_TyCon;
    }

    @Override
    public String toString()
    {
        return "integer<" + val + ">";
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
