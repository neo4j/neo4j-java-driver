/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.Map;

import org.neo4j.driver.v1.Value;

import static java.lang.String.format;

/**
 * INIT request message
 * <p>
 * Sent by clients to initialize a new connection. Must be sent as the very first message after protocol negotiation.
 */
public class InitMessage implements Message
{
    private final String userAgent;
    private Map<String,Value> authToken;

    public InitMessage( String userAgent, Map<String,Value> authToken )
    {
        this.userAgent = userAgent;
        this.authToken = authToken;
    }

    @Override
    public void dispatch( MessageHandler handler ) throws IOException
    {
        handler.handleInitMessage( userAgent, authToken );
    }

    public String userAgent()
    {
        return userAgent;
    }

    public Map<String,Value> authToken()
    {
        return authToken;
    }

    @Override
    public String toString()
    {
        return format( "INIT \"%s\" {...}", userAgent, authToken );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        InitMessage that = (InitMessage) o;

        return !(userAgent != null ? !userAgent.equals( that.userAgent )
                                              : that.userAgent != null);

    }

    @Override
    public int hashCode()
    {
        return userAgent != null ? userAgent.hashCode() : 0;
    }
}
