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
package org.neo4j.driver.internal.summary;

import java.util.Objects;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.summary.ServerInfo;

public class InternalServerInfo implements ServerInfo
{
    private final String address;
    private final String version;

    public InternalServerInfo( BoltServerAddress address, ServerVersion version )
    {
        this.address = address.toString();
        this.version = version.toString();
    }

    @Override
    public String address()
    {
        return address;
    }

    @Override
    public String version()
    {
        return version;
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
        InternalServerInfo that = (InternalServerInfo) o;
        return Objects.equals( address, that.address ) && Objects.equals( version, that.version );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( address, version );
    }

    @Override
    public String toString()
    {
        return "InternalServerInfo{" + "address='" + address + '\'' + ", version='" + version + '\'' + '}';
    }
}
