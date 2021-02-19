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

import java.util.Map;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.Value;

import static java.lang.String.format;

/**
 * SUCCESS response message
 * <p>
 * Sent by the server to signal a successful operation.
 * Terminates response sequence.
 */
public class SuccessMessage implements Message
{
    public final static byte SIGNATURE = 0x70;

    private final Map<String,Value> metadata;

    public SuccessMessage( Map<String,Value> metadata )
    {
        this.metadata = metadata;
    }

    public Map<String,Value> metadata()
    {
        return metadata;
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
    }

    @Override
    public String toString()
    {
        return format( "SUCCESS %s", metadata );
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null && obj.getClass() == getClass();
    }

    @Override
    public int hashCode()
    {
        return 1;
    }
}
