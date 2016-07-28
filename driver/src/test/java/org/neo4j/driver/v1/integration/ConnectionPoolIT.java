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
package org.neo4j.driver.v1.integration;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.DatabaseException;
import org.neo4j.driver.v1.util.TestNeo4j;

import static junit.framework.TestCase.fail;

@Ignore // TODO: re-enable this test when detecting a started server becomes more predictable
public class ConnectionPoolIT
{
    @Rule
    public TestNeo4j neo4j = new TestNeo4j();
    private Driver driver;
    private SessionGrabber sessionGrabber;

    @Test
    public void shouldRecoverFromDownedServer() throws Throwable
    {
        // Given a driver
        driver = GraphDatabase.driver( neo4j.uri() );

        // and given I'm heavily using it to acquire and release sessions
        sessionGrabber = new SessionGrabber( driver );
        sessionGrabber.start();

        // When
        neo4j.restart();

        // Then we accept a hump with failing sessions, but demand that failures stop as soon as the server is back up.
        sessionGrabber.assertSessionsAvailableWithin( 60 );
    }

    @After
    public void cleanup() throws Exception
    {
        sessionGrabber.stop();
        driver.close();
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
                    catch ( ClientException | DatabaseException e )
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

        private void startAndCloseSessions( Driver driver, int sessionCount )
        {
            LinkedList<Session> sessions = new LinkedList<>();
            try
            {
                for ( int i = 0; i < sessionCount; i++ )
                {
                    Session s = driver.session();
                    sessions.add( s );
                    s.run( "RETURN 1" ).consume();
                }
            }
            finally
            {
                for ( Session session : sessions )
                {
                    session.close();
                }
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
