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
package org.neo4j.driver.internal.cluster;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.net.ServerAddressResolver;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;

public class DnsResolver implements ServerAddressResolver
{
    private final Logger logger;

    public DnsResolver( Logging logging )
    {
        this.logger = logging.getLog( DnsResolver.class.getSimpleName() );
    }

    @Override
    public Set<ServerAddress> resolve( ServerAddress initialRouter )
    {
        try
        {
            return Stream.of( InetAddress.getAllByName( initialRouter.host() ) )
                    .map( address -> new BoltServerAddress( initialRouter.host(), address.getHostAddress(), initialRouter.port() ) )
                    .collect( toSet() );
        }
        catch ( UnknownHostException e )
        {
            logger.error( "Failed to resolve address `" + initialRouter + "` to IPs due to error: " + e.getMessage(), e );
            return singleton( initialRouter );
        }
    }
}
