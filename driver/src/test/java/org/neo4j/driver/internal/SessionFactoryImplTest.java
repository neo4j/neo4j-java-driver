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

import org.junit.jupiter.api.Test;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Config;
import org.neo4j.driver.internal.async.LeakLoggingNetworkSession;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.FixedRetryLogic;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

class SessionFactoryImplTest
{
    @Test
    void createsNetworkSessions()
    {
        Config config = Config.builder().withLogging( DEV_NULL_LOGGING ).build();
        SessionFactory factory = newSessionFactory( config );

        NetworkSession readSession = factory.newInstance( builder().withDefaultAccessMode( AccessMode.READ ).build() );
        assertThat( readSession, instanceOf( NetworkSession.class ) );

        NetworkSession writeSession = factory.newInstance( builder().withDefaultAccessMode( AccessMode.WRITE ).build() );
        assertThat( writeSession, instanceOf( NetworkSession.class ) );
    }

    @Test
    void createsLeakLoggingNetworkSessions()
    {
        Config config = Config.builder().withLogging( DEV_NULL_LOGGING ).withLeakedSessionsLogging().build();
        SessionFactory factory = newSessionFactory( config );

        NetworkSession readSession = factory.newInstance( builder().withDefaultAccessMode( AccessMode.READ ).build() );
        assertThat( readSession, instanceOf( LeakLoggingNetworkSession.class ) );

        NetworkSession writeSession = factory.newInstance( builder().withDefaultAccessMode( AccessMode.WRITE ).build() );
        assertThat( writeSession, instanceOf( LeakLoggingNetworkSession.class ) );
    }

    private static SessionFactory newSessionFactory( Config config )
    {
        return new SessionFactoryImpl( mock( ConnectionProvider.class ), new FixedRetryLogic( 0 ), config );
    }
}
