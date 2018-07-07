/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.v1.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.util.DatabaseExtension;
import org.neo4j.driver.v1.util.StubServer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static org.neo4j.driver.v1.Logging.none;
import static org.neo4j.driver.v1.util.Neo4jRunner.DEFAULT_AUTH_TOKEN;

class DriverCloseIT
{
    public abstract static class DriverCloseITBase
    {
        protected abstract Driver createDriver();

        @Test
        void isEncryptedThrowsForClosedDriver()
        {
            Driver driver = createDriver();

            driver.close();

            assertThrows( IllegalStateException.class, driver::isEncrypted );
        }

        @Test
        void sessionThrowsForClosedDriver()
        {
            Driver driver = createDriver();

            driver.close();

            assertThrows( IllegalStateException.class, driver::session );
        }

        @Test
        void sessionWithModeThrowsForClosedDriver()
        {
            Driver driver = createDriver();

            driver.close();

            assertThrows( IllegalStateException.class, () -> driver.session( AccessMode.WRITE ) );
        }

        @Test
        void closeClosedDriver()
        {
            Driver driver = createDriver();

            driver.close();
            driver.close();
            driver.close();
        }
    }

    @Nested
    @TestInstance( Lifecycle.PER_CLASS )
    public class DirectDriverCloseIT extends DriverCloseITBase
    {
        @RegisterExtension
        public final DatabaseExtension neo4j = new DatabaseExtension();

        @Override
        protected Driver createDriver()
        {
            return GraphDatabase.driver( neo4j.uri(), neo4j.authToken() );
        }

        @Test
        void useSessionAfterDriverIsClosed()
        {
            Driver driver = createDriver();
            Session session = driver.session();

            driver.close();

            assertThrows( IllegalStateException.class, () -> session.run( "CREATE ()" ) );
        }
    }

    @Nested
    public class RoutingDriverCloseIT extends DriverCloseITBase
    {
        private StubServer router;

        @BeforeEach
        void setUp() throws Exception
        {
            router = StubServer.start( "acquire_endpoints.script", 9001 );
        }

        @AfterEach
        void tearDown() throws Exception
        {
            if ( router != null )
            {
                router.exitStatus();
            }
        }

        @Override
        protected Driver createDriver()
        {
            Config config = Config.build()
                    .withoutEncryption()
                    .withLogging( none() )
                    .toConfig();

            return GraphDatabase.driver( "bolt+routing://127.0.0.1:9001", DEFAULT_AUTH_TOKEN, config );
        }

        @Test
        void useSessionAfterDriverIsClosed() throws Exception
        {
            StubServer readServer = StubServer.start( "read_server.script", 9005 );

            Driver driver = createDriver();

            try ( Session session = driver.session( AccessMode.READ ) )
            {
                List<Record> records = session.run( "MATCH (n) RETURN n.name" ).list();
                assertEquals( 3, records.size() );
            }

            Session session = driver.session( AccessMode.READ );

            driver.close();

            assertThrows( IllegalStateException.class, () -> session.run( "MATCH (n) RETURN n.name" ) );
            assertEquals( 0, readServer.exitStatus() );
        }
    }
}
