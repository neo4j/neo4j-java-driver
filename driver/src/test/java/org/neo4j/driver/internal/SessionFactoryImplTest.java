/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.driver.internal.retry.FixedRetryLogic;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Session;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

public class SessionFactoryImplTest
{
    @Test
    public void createsNetworkSessions()
    {
        Config config = Config.build().withLogging( DEV_NULL_LOGGING ).toConfig();
        SessionFactory factory = newSessionFactory( config );

        Session readSession = factory.newInstance( AccessMode.READ, null );
        assertThat( readSession, instanceOf( NetworkSession.class ) );

        Session writeSession = factory.newInstance( AccessMode.WRITE, null );
        assertThat( writeSession, instanceOf( NetworkSession.class ) );
    }

    @Test
    public void createsLeakLoggingNetworkSessions()
    {
        Config config = Config.build().withLogging( DEV_NULL_LOGGING ).withLeakedSessionsLogging().toConfig();
        SessionFactory factory = newSessionFactory( config );

        Session readSession = factory.newInstance( AccessMode.READ, null );
        assertThat( readSession, instanceOf( LeakLoggingNetworkSession.class ) );

        Session writeSession = factory.newInstance( AccessMode.WRITE, null );
        assertThat( writeSession, instanceOf( LeakLoggingNetworkSession.class ) );
    }

    private static SessionFactory newSessionFactory( Config config )
    {
        return new SessionFactoryImpl( mock( ConnectionProvider.class ), new FixedRetryLogic( 0 ), config );
    }
}
