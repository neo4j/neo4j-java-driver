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

import org.neo4j.driver.internal.messaging.Message;

/**
 * IGNORED response message
 * <p>
 * Sent by the server to signal that an operation has been ignored.
 * Terminates response sequence.
 */
public class IgnoredMessage implements Message
{
    public final static byte SIGNATURE = 0x7E;

    public static final IgnoredMessage IGNORED = new IgnoredMessage();

    private IgnoredMessage()
    {
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
    }

    @Override
    public String toString()
    {
        return "IGNORED {}";
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
