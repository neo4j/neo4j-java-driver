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

package org.neo4j.driver.internal.summary;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.summary.ServerInfo;

public class InternalServerInfo implements ServerInfo
{
    private final BoltServerAddress address;
    private final String version;

    public InternalServerInfo( BoltServerAddress address, String version )
    {
        this.address = address;
        this.version = version;
    }

    public BoltServerAddress boltServerAddress()
    {
        return this.address;
    }

    @Override
    public String address()
    {
        return this.address.toString();
    }

    @Override
    public String version()
    {
        return version;
    }
}
