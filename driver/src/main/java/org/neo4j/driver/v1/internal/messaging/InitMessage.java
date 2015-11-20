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
package org.neo4j.driver.v1.internal.messaging;

import java.io.IOException;

import static java.lang.String.format;

/**
 * INIT request message
 * <p>
 * Sent by clients to initialize a new connection. Must be sent as the very first message after protocol negotiation.
 */
public class InitMessage implements Message
{
    private final String clientNameAndVersion;

    public InitMessage( String clientNameAndVersion )
    {
        this.clientNameAndVersion = clientNameAndVersion;
    }

    @Override
    public void dispatch( MessageHandler handler ) throws IOException
    {
        handler.handleInitMessage( clientNameAndVersion );
    }

    @Override
    public String toString()
    {
        return format( "[INIT \"%s\"]", clientNameAndVersion );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        InitMessage that = (InitMessage) o;

        return !(clientNameAndVersion != null ? !clientNameAndVersion.equals( that.clientNameAndVersion )
                                              : that.clientNameAndVersion != null);

    }

    @Override
    public int hashCode()
    {
        return clientNameAndVersion != null ? clientNameAndVersion.hashCode() : 0;
    }
}
