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
import java.util.Map;

import org.neo4j.driver.v1.Value;

import static java.lang.String.format;

/**
 * RUN request message
 * <p>
 * Sent by clients to start a new Tank job for a given statement and
 * parameter set.
 */
public class RunMessage implements Message
{
    private final String statement;
    private final Map<String,Value> parameters;

    public RunMessage( String statement, Map<String,Value> parameters )
    {
        this.statement = statement;
        this.parameters = parameters;
    }

    @Override
    public void dispatch( MessageHandler handler ) throws IOException
    {
        handler.handleRunMessage( statement, parameters );
    }

    @Override
    public String toString()
    {
        return format( "[RUN \"%s\" %s]", statement, parameters );
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

        RunMessage that = (RunMessage) o;

        if ( parameters != null ? !parameters.equals( that.parameters ) : that.parameters != null )
        {
            return false;
        }
        if ( statement != null ? !statement.equals( that.statement ) : that.statement != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = statement != null ? statement.hashCode() : 0;
        result = 31 * result + (parameters != null ? parameters.hashCode() : 0);
        return result;
    }
}
