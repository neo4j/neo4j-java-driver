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
package org.neo4j.driver.internal.messaging.request;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.DatabaseName;

import static java.util.Collections.emptyMap;
import static org.neo4j.driver.Values.ofValue;
import static org.neo4j.driver.internal.messaging.request.TransactionMetadataBuilder.buildMetadata;

public class RunWithMetadataMessage extends MessageWithMetadata
{
    public final static byte SIGNATURE = 0x10;

    private final String query;
    private final Map<String,Value> parameters;

    public static RunWithMetadataMessage autoCommitTxRunMessage(Query query, TransactionConfig config, DatabaseName databaseName, AccessMode mode,
            Bookmark bookmark )
    {
        return autoCommitTxRunMessage(query, config.timeout(), config.metadata(), databaseName, mode, bookmark );
    }

    public static RunWithMetadataMessage autoCommitTxRunMessage(Query query, Duration txTimeout, Map<String,Value> txMetadata, DatabaseName databaseName,
            AccessMode mode, Bookmark bookmark )
    {
        Map<String,Value> metadata = buildMetadata( txTimeout, txMetadata, databaseName, mode, bookmark );
        return new RunWithMetadataMessage( query.text(), query.parameters().asMap( ofValue() ), metadata );
    }

    public static RunWithMetadataMessage unmanagedTxRunMessage(Query query)
    {
        return new RunWithMetadataMessage( query.text(), query.parameters().asMap( ofValue() ), emptyMap() );
    }

    private RunWithMetadataMessage(String query, Map<String,Value> parameters, Map<String,Value> metadata )
    {
        super( metadata );
        this.query = query;
        this.parameters = parameters;
    }

    public String query()
    {
        return query;
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
        return Objects.equals(query, that.query) && Objects.equals( parameters, that.parameters ) && Objects.equals( metadata(), that.metadata() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query, parameters, metadata() );
    }

    @Override
    public String toString()
    {
        return "RUN \"" + query + "\" " + parameters + " " + metadata();
    }
}
