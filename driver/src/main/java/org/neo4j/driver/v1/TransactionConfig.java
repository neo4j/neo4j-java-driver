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
package org.neo4j.driver.v1;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import org.neo4j.driver.internal.util.Extract;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.util.Preconditions.checkArgument;

public class TransactionConfig
{
    private static final TransactionConfig EMPTY = builder().build();

    private final Duration timeout;
    private final Map<String,Value> metadata;

    private TransactionConfig( Builder builder )
    {
        this.timeout = builder.timeout;
        this.metadata = unmodifiableMap( builder.metadata );
    }

    public static TransactionConfig empty()
    {
        return EMPTY;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public Duration timeout()
    {
        return timeout;
    }

    public Map<String,Value> metadata()
    {
        return metadata;
    }

    public boolean isEmpty()
    {
        return timeout == null && (metadata == null || metadata.isEmpty());
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
        TransactionConfig that = (TransactionConfig) o;
        return Objects.equals( timeout, that.timeout ) &&
               Objects.equals( metadata, that.metadata );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( timeout, metadata );
    }

    @Override
    public String toString()
    {
        return "TransactionConfig{" +
               "timeout=" + timeout +
               ", metadata=" + metadata +
               '}';
    }

    public static class Builder
    {
        private Duration timeout;
        private Map<String,Value> metadata = emptyMap();

        private Builder()
        {
        }

        public Builder withTimeout( Duration timeout )
        {
            requireNonNull( timeout, "Transaction timeout should not be null" );
            checkArgument( !timeout.isZero(), "Transaction timeout should not be zero" );
            checkArgument( !timeout.isNegative(), "Transaction timeout should not be negative" );

            this.timeout = timeout;
            return this;
        }

        public Builder withMetadata( Map<String,Object> metadata )
        {
            requireNonNull( metadata, "Transaction metadata should not be null" );

            this.metadata = Extract.mapOfValues( metadata );
            return this;
        }

        public TransactionConfig build()
        {
            return new TransactionConfig( this );
        }
    }
}
