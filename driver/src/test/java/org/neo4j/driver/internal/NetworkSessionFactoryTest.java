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
package org.neo4j.driver.internal;

import org.junit.Test;

import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Session;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

public class NetworkSessionFactoryTest
{
    @Test
    public void createsNetworkSessions()
    {
        SessionFactory factory = new NetworkSessionFactory( mock( ConnectionProvider.class ), DEV_NULL_LOGGING );

        Session readSession = factory.newInstance( AccessMode.READ );
        assertThat( readSession, instanceOf( NetworkSession.class ) );

        Session writeSession = factory.newInstance( AccessMode.WRITE );
        assertThat( writeSession, instanceOf( NetworkSession.class ) );
    }
}
