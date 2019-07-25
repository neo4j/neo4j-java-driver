/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalBookmark;

import static org.neo4j.driver.internal.messaging.request.TransactionMetadataBuilder.buildMetadata;

public class BeginMessage extends MessageWithMetadata
{
    public static final byte SIGNATURE = 0x11;

    public BeginMessage( InternalBookmark bookmark, TransactionConfig config, String databaseName, AccessMode mode )
    {
        this( bookmark, config.timeout(), config.metadata(), mode, databaseName );
    }

    public BeginMessage( InternalBookmark bookmark, Duration txTimeout, Map<String,Value> txMetadata, AccessMode mode, String databaseName )
    {
        super( buildMetadata( txTimeout, txMetadata, databaseName, mode, bookmark ) );
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
        BeginMessage that = (BeginMessage) o;
        return Objects.equals( metadata(), that.metadata() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( metadata() );
    }

    @Override
    public String toString()
    {
        return "BEGIN " + metadata();
    }
}
