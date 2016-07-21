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

package org.neo4j.driver.internal.net;

import org.junit.Test;
import org.neo4j.driver.internal.net.BoltServerAddress;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class BoltServerAddressTest
{
    @Test
    public void variantsOfLocalHostShouldResolveAsLocal() throws Exception
    {
        assertThat( new BoltServerAddress( "localhost", 7687 ).isLocal(), equalTo( true ) );
        assertThat( new BoltServerAddress( "LocalHost", 7687 ).isLocal(), equalTo( true ) );
        assertThat( new BoltServerAddress( "LOCALHOST", 7687 ).isLocal(), equalTo( true ) );
        assertThat( new BoltServerAddress( "127.0.0.1", 7687 ).isLocal(), equalTo( true ) );
        assertThat( new BoltServerAddress( "127.5.6.7", 7687 ).isLocal(), equalTo( true ) );
        assertThat( new BoltServerAddress( "x", 7687 ).isLocal(), equalTo( false ) );
    }

    @Test
    public void defaultPortShouldBe7687()
    {
        assertThat( BoltServerAddress.DEFAULT_PORT, equalTo( 7687 ) );
    }

    @Test
    public void portShouldUseDefaultIfNotSupplied()
    {
        assertThat( new BoltServerAddress( "localhost" ).port(), equalTo( BoltServerAddress.DEFAULT_PORT ) );
    }

}