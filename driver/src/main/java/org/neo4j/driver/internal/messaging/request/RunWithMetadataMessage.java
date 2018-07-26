/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.internal.messaging.request;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.Value;

public class RunWithMetadataMessage extends TransactionStartingMessage
{
    public final static byte SIGNATURE = 0x10;

    private final String statement;
    private final Map<String,Value> parameters;

    public RunWithMetadataMessage( String statement, Map<String,Value> parameters, Bookmarks bookmarks, TransactionConfig config )
    {
        this( statement, parameters, bookmarks, config.timeout(), config.metadata() );
    }

    public RunWithMetadataMessage( String statement, Map<String,Value> parameters, Bookmarks bookmarks, Duration txTimeout, Map<String,Value> txMetadata )
    {
        super( bookmarks, txTimeout, txMetadata );
        this.statement = statement;
        this.parameters = parameters;
    }

    public String statement()
    {
        return statement;
    }

    public Map<String,Value> parameters()
    {
        return parameters;
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
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
        RunWithMetadataMessage that = (RunWithMetadataMessage) o;
        return Objects.equals( statement, that.statement ) &&
               Objects.equals( parameters, that.parameters ) &&
               Objects.equals( metadata, that.metadata );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( statement, parameters, metadata );
    }

    @Override
    public String toString()
    {
        return "RUN \"" + statement + "\" " + parameters + " " + metadata;
    }
}
