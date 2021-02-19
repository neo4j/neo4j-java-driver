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
package org.neo4j.driver.internal.messaging.response;

import java.util.Arrays;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.Value;

public class RecordMessage implements Message
{
    public final static byte SIGNATURE = 0x71;

    private final Value[] fields;

    public RecordMessage( Value[] fields )
    {
        this.fields = fields;
    }

    public Value[] fields()
    {
        return fields;
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
    }

    @Override
    public String toString()
    {
        return "RECORD " + Arrays.toString( fields );
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

        RecordMessage that = (RecordMessage) o;

        return Arrays.equals( fields, that.fields );
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( fields );
    }
}
