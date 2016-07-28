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
package org.neo4j.driver.internal.messaging;

import java.io.IOException;

/**
 * ACK_FAILURE request message
 *
 * This message acts as a barrier after an error, informing the server that we've seen the error
 * message, and that messages that follow this one are safe to execute.
 */
public class AckFailureMessage implements Message
{
    public static final AckFailureMessage ACK_FAILURE= new AckFailureMessage();

    @Override
    public void dispatch( MessageHandler handler ) throws IOException
    {
        handler.handleAckFailureMessage();
    }

    @Override
    public String toString()
    {
        return "[ACK_FAILURE]";
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
