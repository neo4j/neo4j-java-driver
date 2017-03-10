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
package org.neo4j.driver.internal.cluster;

import org.junit.Test;

import java.util.Set;

import org.neo4j.driver.internal.net.BoltServerAddress;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.internal.cluster.DnsResolver.DEFAULT;

public class DnsResolverTest
{
    @Test
    public void shouldResolveDNSToIPs()
    {
        Set<BoltServerAddress> resolve = DEFAULT.resolve( new BoltServerAddress( "google.com", 80 ) );
        assertThat( resolve.size(), greaterThanOrEqualTo( 1 ) );
    }

    @Test
    public void shouldResolveLocalhostDNSToIPs()
    {
        Set<BoltServerAddress> resolve = DEFAULT.resolve( new BoltServerAddress( "127.0.0.1", 80 ) );
        assertThat( resolve.size(), greaterThanOrEqualTo( 1 ) );
    }
}
