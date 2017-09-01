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
package org.neo4j.driver.v1.integration;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.ConnectionTrackingDriverFactory;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.DatabaseException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.util.TestNeo4j;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.internal.retry.RetrySettings.DEFAULT;

public class ConnectionPoolIT
{
    @Rule
    public final TestNeo4j neo4j = new TestNeo4j();

    private Driver driver;
    private SessionGrabber sessionGrabber;

    @Test
    public void shouldRecoverFromDownedServer() throws Throwable
    {
        // Given a driver
        driver = GraphDatabase.driver( neo4j.uri(), neo4j.authToken() );

        // and given I'm heavily using it to acquire and release sessions
        sessionGrabber = new SessionGrabber( driver );
        sessionGrabber.start();

        // When
        neo4j.restartDb();

        // Then we accept a hump with failing sessions, but demand that failures stop as soon as the server is back up.
        sessionGrabber.assertSessionsAvailableWithin( 60 );
    }

    @Test
    public void shouldDisposeConnectionsBasedOnMaxLifetime()
    {
        FakeClock clock = new FakeClock();
        ConnectionTrackingDriverFactory driverFactory = new ConnectionTrackingDriverFactory( clock );

        int maxConnLifetimeHours = 3;
        Config config = Config.build().withMaxConnectionLifetime( maxConnLifetimeHours, TimeUnit.HOURS ).toConfig();
        RoutingSettings routingSettings = new RoutingSettings( 1, 1 );
        driver = driverFactory.newInstance( neo4j.uri(), neo4j.authToken(), routingSettings, DEFAULT, config );

        // force driver create two connections and return them to the connection pool
        startAndCloseSessions( driver, 3 );

        // verify that two connections were created, they should be open and idle in the pool
        List<Connection> connections1 = driverFactory.connections();
        assertEquals( 3, connections1.size() );
        assertTrue( connections1.get( 0 ).isOpen() );
        assertTrue( connections1.get( 1 ).isOpen() );
        assertTrue( connections1.get( 2 ).isOpen() );

        // move the clock forward so that two idle connections seem too old
        clock.progress( TimeUnit.HOURS.toMillis( maxConnLifetimeHours + 1 ) );

        // force driver to acquire new connection and put it back to the pool
        startAndCloseSessions( driver, 1 );

        // all existing connections should be closed because they are too old, new connection was created
        List<Connection> connections2 = driverFactory.connections();
        assertEquals( 4, connections2.size() );
        assertFalse( connections2.get( 0 ).isOpen() );
        assertFalse( connections2.get( 1 ).isOpen() );
        assertFalse( connections2.get( 2 ).isOpen() );
        assertTrue( connections2.get( 3 ).isOpen() );
    }

    @After
    public void cleanup() throws Exception
    {
        if ( driver != null )
        {
            driver.close();
        }

        if ( sessionGrabber != null )
        {
            sessionGrabber.stop();
        }
    }

    private static void startAndCloseSessions( Driver driver, int sessionCount )
    {
        List<Session> sessions = new ArrayList<>( sessionCount );
        List<StatementResult> results = new ArrayList<>( sessionCount );
        try
        {
            for ( int i = 0; i < sessionCount; i++ )
            {
                Session session = driver.session();
                sessions.add( session );

                StatementResult result = session.run( "RETURN 1" );
                results.add( result );
            }
        }
        finally
        {
            for ( StatementResult result : results )
            {
                result.consume();
            }
            for ( Session session : sessions )
            {
                session.close();
            }
        }
    }

    /**
     * This is a background runner that will grab lots of sessions in one go, and then close them all, while tracking
     * it's current state - is it currently able to acquire complete groups of sessions, or are there errors occurring?
     *
     * This can thus be used to judge the state of the driver - is it currently healthy or not?
     */
    private class SessionGrabber implements Runnable
    {
        private final Driver driver;
        private final CountDownLatch stopped = new CountDownLatch( 1 );
        private volatile boolean sessionsAreAvailable = false;
        private volatile boolean run = true;
        private volatile Throwable lastExceptionFromDriver;

        public SessionGrabber( Driver driver )
        {
            this.driver = driver;
        }

        public void start()
        {
            new Thread(this).start();
        }

        @Override
        public void run()
        {
            try
            {
                while ( run )
                {
                    try
                    {
                        // Try and launch 8 concurrent sessions
                        startAndCloseSessions( driver, 8 );

                        // Success! We created 8 sessions without failures
                        sessionsAreAvailable = true;
                    }
                    catch ( ClientException | DatabaseException | SessionExpiredException |
                            ServiceUnavailableException e )
                    {
                        lastExceptionFromDriver = e;
                        sessionsAreAvailable = false;
                    }
                    catch ( Throwable e )
                    {
                        e.printStackTrace();
                        lastExceptionFromDriver = e;
                        throw new RuntimeException( e );
                    }
                }
            } finally
            {
                stopped.countDown();
            }
        }

        public void assertSessionsAvailableWithin( int timeoutSeconds ) throws InterruptedException
        {
            long deadline = System.currentTimeMillis() + 1000 * timeoutSeconds;
            while( System.currentTimeMillis() < deadline )
            {
                if( sessionsAreAvailable )
                {
                    // Success!
                    return;
                }
                Thread.sleep( 100 );
            }

            // Failure - timeout :(
            lastExceptionFromDriver.printStackTrace();
            fail( "sessions did not become available from the driver after the db restart within the specified " +
                  "timeout. Last failure was: " + lastExceptionFromDriver.getMessage() );
        }

        public void stop() throws InterruptedException
        {
            run = false;
            stopped.await(10, TimeUnit.SECONDS );
        }
    }
}
