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
package org.neo4j.driver.internal;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class InternalDatabaseName implements DatabaseName
{
    private final String databaseName;

    InternalDatabaseName( String databaseName )
    {
        this.databaseName = requireNonNull( databaseName );
    }

    @Override
    public Optional<String> databaseName()
    {
        return Optional.of( databaseName );
    }

    @Override
    public String description()
    {
        return databaseName;
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
        InternalDatabaseName that = (InternalDatabaseName) o;
        return databaseName.equals( that.databaseName );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseName );
    }

    @Override
    public String toString()
    {
        return "InternalDatabaseName{" + "databaseName='" + databaseName + '\'' + '}';
    }
}
