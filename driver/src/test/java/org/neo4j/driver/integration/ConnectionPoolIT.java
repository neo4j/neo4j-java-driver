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
package org.neo4j.driver.integration;

import io.netty.channel.Channel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.io.ChannelTrackingDriverFactory;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.internal.retry.RetrySettings.DEFAULT;
import static org.neo4j.driver.internal.util.Matchers.connectionAcquisitionTimeoutError;

@ParallelizableIT
class ConnectionPoolIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Driver driver;
    private SessionGrabber sessionGrabber;

    @AfterEach
    void cleanup() throws Exception
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

    @Test
    void shouldRecoverFromDownedServer() throws Throwable
    {
        // Given a driver
        driver = GraphDatabase.driver( neo4j.uri(), neo4j.authToken() );

        // and given I'm heavily using it to acquire and release sessions
        sessionGrabber = new SessionGrabber( driver );
        sessionGrabber.start();

        // When
        neo4j.forceRestartDb();

        // Then we accept a hump with failing sessions, but demand that failures stop as soon as the server is back up.
        sessionGrabber.assertSessionsAvailableWithin( 120 );
    }

    @Test
    void shouldDisposeChannelsBasedOnMaxLifetime() throws Exception
    {
        FakeClock clock = new FakeClock();
        ChannelTrackingDriverFactory driverFactory = new ChannelTrackingDriverFactory( clock );

        int maxConnLifetimeHours = 3;
        Config config = Config.builder().withMaxConnectionLifetime( maxConnLifetimeHours, TimeUnit.HOURS ).build();
        driver = driverFactory.newInstance( neo4j.uri(), neo4j.authToken(), RoutingSettings.DEFAULT, DEFAULT, config, SecurityPlanImpl.insecure() );

        // force driver create channel and return it to the pool
        startAndCloseTransactions( driver, 1 );

        // verify that channel was created, it should be open and idle in the pool
        List<Channel> channels1 = driverFactory.channels();
        assertEquals( 1, channels1.size() );
        assertTrue( channels1.get( 0 ).isActive() );

        // await channel to be returned to the pool
        awaitNoActiveChannels( driverFactory, 20, SECONDS );
        // move the clock forward so that idle channel seem too old
        clock.progress( TimeUnit.HOURS.toMillis( maxConnLifetimeHours + 1 ) );

        // force driver to acquire new connection and put it back to the pool
        startAndCloseTransactions( driver, 1 );

        // old existing channel should not be reused because it is too old
        List<Channel> channels2 = driverFactory.channels();
        assertEquals( 2, channels2.size() );

        Channel channel1 = channels2.get( 0 );
        Channel channel2 = channels2.get( 1 );

        // old existing should be closed in reasonable time
        assertTrue( channel1.closeFuture().await( 20, SECONDS ) );
        assertFalse( channel1.isActive() );

        // new channel should remain open and idle in the pool
        assertTrue( channel2.isActive() );
    }

    @Test
    void shouldRespectMaxConnectionPoolSize()
    {
        int maxPoolSize = 3;
        Config config = Config.builder()
                .withMaxConnectionPoolSize( maxPoolSize )
                .withConnectionAcquisitionTimeout( 542, TimeUnit.MILLISECONDS )
                .withEventLoopThreads( 1 )
                .build();

        driver = GraphDatabase.driver( neo4j.uri(), neo4j.authToken(), config );

        ClientException e = assertThrows( ClientException.class, () -> startAndCloseTransactions( driver, maxPoolSize + 1 ) );
        assertThat( e, is( connectionAcquisitionTimeoutError( 542 ) ) );

    }

    private static void startAndCloseTransactions( Driver driver, int txCount )
    {
        List<Session> sessions = new ArrayList<>( txCount );
        List<Transaction> transactions = new ArrayList<>( txCount );
        List<Result> results = new ArrayList<>( txCount );
        try
        {
            for ( int i = 0; i < txCount; i++ )
            {
                Session session = driver.session();
                sessions.add( session );

                Transaction tx = session.beginTransaction();
                transactions.add( tx );

                Result result = tx.run( "RETURN 1" );
                results.add( result );
            }
        }
        finally
        {
            for ( Result result : results )
            {
                result.consume();
            }
            for ( Transaction tx : transactions )
            {
                tx.commit();
            }
            for ( Session session : sessions )
            {
                session.close();
            }
        }
    }

    private void awaitNoActiveChannels( ChannelTrackingDriverFactory driverFactory, long value, TimeUnit unit )
            throws InterruptedException
    {
        long deadline = System.currentTimeMillis() + unit.toMillis( value );
        int activeChannels = -1;
        while ( System.currentTimeMillis() < deadline )
        {
            activeChannels = driverFactory.activeChannels( neo4j.address() );
            if ( activeChannels == 0 )
            {
                return;
            }
            else
            {
                Thread.sleep( 100 );
            }
        }
        throw new AssertionError( "Active channels present: " + activeChannels );
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
        private final int sleepTimeout = 100;

        SessionGrabber( Driver driver )
        {
            this.driver = driver;
        }

        public void start()
        {
            new Thread( this ).start();
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
                        startAndCloseTransactions( driver, 8 );

                        // Success! We created 8 sessions without failures
                        sessionsAreAvailable = true;
                    }
                    catch ( Throwable e )
                    {
                        lastExceptionFromDriver = e;
                        sessionsAreAvailable = false;
                    }
                    try
                    {
                        Thread.sleep( sleepTimeout );
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            } finally
            {
                stopped.countDown();
            }
        }

        void assertSessionsAvailableWithin( int timeoutSeconds ) throws InterruptedException
        {
            long deadline = System.currentTimeMillis() + 1000 * timeoutSeconds;
            while( System.currentTimeMillis() < deadline )
            {
                if( sessionsAreAvailable )
                {
                    // Success!
                    return;
                }
                Thread.sleep( sleepTimeout );
            }

            // Failure - timeout :(
            lastExceptionFromDriver.printStackTrace();
            fail( "sessions did not become available from the driver after the db restart within the specified " +
                  "timeout. Last failure was: " + lastExceptionFromDriver.getMessage() );
        }

        public void stop() throws InterruptedException
        {
            run = false;
            stopped.await( 10, SECONDS );
        }
    }
}
