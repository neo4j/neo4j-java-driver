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
package org.neo4j.driver.internal.value;

import java.util.Arrays;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Type;

public class BytesValue extends ValueAdapter
{
    private final byte[] val;

    public BytesValue( byte[] val )
    {
        if ( val == null )
        {
            throw new IllegalArgumentException( "Cannot construct BytesValue from null" );
        }
        this.val = val;
    }

    @Override
    public boolean isEmpty()
    {
        return val.length == 0;
    }

    @Override
    public int size()
    {
        return val.length;
    }

    @Override
    public byte[] asObject()
    {
        return val;
    }

    @Override
    public byte[] asByteArray()
    {
        return val;
    }

    @Override
    public Type type()
    {
        return InternalTypeSystem.TYPE_SYSTEM.BYTES();
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

        BytesValue values = (BytesValue) o;
        return Arrays.equals(val, values.val);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(val);
    }

    @Override
    public String toString()
    {
        StringBuilder s = new StringBuilder("#");
        for (byte b : val)
        {
            if (b < 0x10)
            {
                s.append('0');
            }
            s.append(Integer.toHexString(b));
        }
        return s.toString();
    }
}
