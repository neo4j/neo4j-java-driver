/*
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
package org.neo4j.driver.v1;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.logging.Level;

import org.neo4j.driver.internal.logging.ConsoleLogging;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.util.StubServer;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith( Enclosed.class )
public class DriverCloseIT
{
    public abstract static class DriverCloseITBase
    {
        protected abstract Driver createDriver();

        @Test
        public void isEncryptedThrowsForClosedDriver()
        {
            Driver driver = createDriver();

            driver.close();

            try
            {
                driver.isEncrypted();
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( IllegalStateException.class ) );
            }
        }

        @Test
        public void sessionThrowsForClosedDriver()
        {
            Driver driver = createDriver();

            driver.close();

            try
            {
                driver.session();
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( IllegalStateException.class ) );
            }
        }

        @Test
        public void sessionWithModeThrowsForClosedDriver()
        {
            Driver driver = createDriver();

            driver.close();

            try
            {
                driver.session( AccessMode.WRITE );
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( IllegalStateException.class ) );
            }
        }

        @Test
        public void closeClosedDriver()
        {
            Driver driver = createDriver();

            driver.close();
            driver.close();
            driver.close();
        }
    }

    public static class DirectDriverCloseIT extends DriverCloseITBase
    {
        @ClassRule
        public static TestNeo4j neo4j = new TestNeo4j();

        @Override
        protected Driver createDriver()
        {
            return GraphDatabase.driver( neo4j.uri() );
        }

        @Test
        public void useSessionAfterDriverIsClosed()
        {
            Driver driver = createDriver();
            Session session = driver.session();

            driver.close();

            try
            {
                session.run( "create ()" );
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( ServiceUnavailableException.class ) );
            }
        }
    }

    public static class RoutingDriverCloseIT extends DriverCloseITBase
    {
        private StubServer router;

        @Before
        public void setUp() throws Exception
        {
            router = StubServer.start( "acquire_endpoints.script", 9001 );
        }

        @After
        public void tearDown() throws Exception
        {
            router.exitStatus();
        }

        @Override
        protected Driver createDriver()
        {
            Config config = Config.build()
                    .withEncryptionLevel( Config.EncryptionLevel.NONE )
                    .withLogging( new ConsoleLogging( Level.OFF ) )
                    .toConfig();

            return GraphDatabase.driver( "bolt+routing://127.0.0.1:9001", config );
        }

        @Test
        public void useSessionAfterDriverIsClosed() throws Exception
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

            try
            {
                session.run( "MATCH (n) RETURN n.name" );
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e.getCause(), instanceOf( ServiceUnavailableException.class ) );
            }

            assertEquals( 0, readServer.exitStatus() );
        }
    }
}
