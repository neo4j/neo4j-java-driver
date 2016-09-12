/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

package org.neo4j.driver.internal;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Session;

abstract class BaseDriver implements Driver
{
    private final SecurityPlan securityPlan;
    protected final Logger log;
    protected final Set<BoltServerAddress> servers = new HashSet<>();

    BaseDriver( BoltServerAddress address, SecurityPlan securityPlan, Logging logging )
    {
        this.servers.add( address );
        this.securityPlan = securityPlan;
        this.log = logging.getLog( Session.LOG_NAME );
    }

    @Override
    public boolean isEncrypted()
    {
        return securityPlan.requiresEncryption();
    }

    Set<BoltServerAddress> servers()
    {
        return Collections.unmodifiableSet( servers );
    }

    //This is somewhat silly and has O(n) complexity
    protected BoltServerAddress randomServer()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int item = random.nextInt(servers.size());
        int i = 0;
        for ( BoltServerAddress server : servers )
        {
            if (i == item)
            {
                return server;
            }
        }

        throw new IllegalStateException( "This cannot happen" );
    }

}
