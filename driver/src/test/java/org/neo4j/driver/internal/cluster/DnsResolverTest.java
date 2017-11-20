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

import java.net.UnknownHostException;
import java.util.Set;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.v1.Logger;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DnsResolverTest
{
    private DnsResolver resolver = new DnsResolver( mock( Logger.class ) );

    @Test
    public void shouldResolveDNSToIPs()
    {
        Set<BoltServerAddress> resolve = resolver.resolve( new BoltServerAddress( "google.com", 80 ) );
        assertThat( resolve.size(), greaterThanOrEqualTo( 1 ) );
    }

    @Test
    public void shouldResolveLocalhostIPDNSToIPs()
    {
        Set<BoltServerAddress> resolve = resolver.resolve( new BoltServerAddress( "127.0.0.1", 80 ) );
        assertThat( resolve.size(), greaterThanOrEqualTo( 1 ) );
    }

    @Test
    public void shouldResolveLocalhostDNSToIPs()
    {
        Set<BoltServerAddress> resolve = resolver.resolve( new BoltServerAddress( "localhost", 80 ) );
        assertThat( resolve.size(), greaterThanOrEqualTo( 1 ) );
    }

    @Test
    public void shouldResolveIPv6LocalhostDNSToIPs()
    {
        Set<BoltServerAddress> resolve = resolver.resolve( new BoltServerAddress( "[::1]", 80 ) );
        assertThat( resolve.size(), greaterThanOrEqualTo( 1 ) );
    }

    @Test
    public void shouldExceptionAndGiveDefaultValue()
    {
        Logger logger = mock( Logger.class );
        DnsResolver resolver = new DnsResolver( logger );
        Set<BoltServerAddress> resolve = resolver.resolve( new BoltServerAddress( "[/]", 80 ) );
        verify( logger ).error( any( String.class ), any( UnknownHostException.class ) );
        assertThat( resolve.size(), greaterThanOrEqualTo( 1 ) );
    }
}
