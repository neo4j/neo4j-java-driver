/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.internal.util.Matchers.connectionAcquisitionTimeoutError;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.io.ChannelTrackingDriverFactory;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class ConnectionPoolIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Driver driver;
    private SessionGrabber sessionGrabber;

    @AfterEach
    void cleanup() throws Exception {
        if (driver != null) {
            driver.close();
        }

        if (sessionGrabber != null) {
            sessionGrabber.stop();
        }
    }

    @Test
    void shouldRecoverFromDownedServer() throws Throwable {
        // Given a driver
        driver = GraphDatabase.driver(neo4j.uri(), neo4j.authTokenManager());

        // and given I'm heavily using it to acquire and release sessions
        sessionGrabber = new SessionGrabber(driver);
        sessionGrabber.start();

        // When
        neo4j.stopProxy();
        neo4j.startProxy();

        // Then we accept a hump with failing sessions, but demand that failures stop as soon as the server is back up.
        sessionGrabber.assertSessionsAvailableWithin();
    }

    @Test
    void shouldDisposeChannelsBasedOnMaxLifetime() throws Exception {
        var clock = new FakeClock();
        var driverFactory = new ChannelTrackingDriverFactory(clock);

        var maxConnLifetimeHours = 3;
        var config = Config.builder()
                .withMaxConnectionLifetime(maxConnLifetimeHours, TimeUnit.HOURS)
                .build();
        driver = driverFactory.newInstance(
                neo4j.uri(), neo4j.authTokenManager(), config, SecurityPlanImpl.insecure(), null, null);

        // force driver create channel and return it to the pool
        startAndCloseTransactions(driver, 1);

        // verify that channel was created, it should be open and idle in the pool
        var channels1 = driverFactory.channels();
        assertEquals(1, channels1.size());
        assertTrue(channels1.get(0).isActive());

        // await channel to be returned to the pool
        awaitNoActiveChannels(driverFactory);
        // move the clock forward so that idle channel seem too old
        clock.progress(TimeUnit.HOURS.toMillis(maxConnLifetimeHours + 1));

        // force driver to acquire new connection and put it back to the pool
        startAndCloseTransactions(driver, 1);

        // old existing channel should not be reused because it is too old
        var channels2 = driverFactory.channels();
        assertEquals(2, channels2.size());

        var channel1 = channels2.get(0);
        var channel2 = channels2.get(1);

        // old existing should be closed in reasonable time
        assertTrue(channel1.closeFuture().await(20, SECONDS));
        assertFalse(channel1.isActive());

        // new channel should remain open and idle in the pool
        assertTrue(channel2.isActive());
    }

    @Test
    void shouldRespectMaxConnectionPoolSize() {
        var maxPoolSize = 3;
        var config = Config.builder()
                .withMaxConnectionPoolSize(maxPoolSize)
                .withConnectionAcquisitionTimeout(542, TimeUnit.MILLISECONDS)
                .withEventLoopThreads(1)
                .build();

        driver = GraphDatabase.driver(neo4j.uri(), neo4j.authTokenManager(), config);

        var e = assertThrows(ClientException.class, () -> startAndCloseTransactions(driver, maxPoolSize + 1));
        assertThat(e, is(connectionAcquisitionTimeoutError(542)));
    }

    private static void startAndCloseTransactions(Driver driver, int txCount) {
        List<Session> sessions = new ArrayList<>(txCount);
        List<Transaction> transactions = new ArrayList<>(txCount);
        List<Result> results = new ArrayList<>(txCount);
        try {
            for (var i = 0; i < txCount; i++) {
                var session = driver.session();
                sessions.add(session);

                var tx = session.beginTransaction();
                transactions.add(tx);

                var result = tx.run("RETURN 1");
                results.add(result);
            }
        } finally {
            for (var result : results) {
                result.consume();
            }
            for (var tx : transactions) {
                tx.commit();
            }
            for (var session : sessions) {
                session.close();
            }
        }
    }

    @SuppressWarnings("BusyWait")
    private void awaitNoActiveChannels(ChannelTrackingDriverFactory driverFactory) throws InterruptedException {
        var deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(20);
        var activeChannels = -1;
        while (System.currentTimeMillis() < deadline) {
            activeChannels = driverFactory.activeChannels(neo4j.address());
            if (activeChannels == 0) {
                return;
            } else {
                Thread.sleep(100);
            }
        }
        throw new AssertionError("Active channels present: " + activeChannels);
    }

    /**
     * This is a background runner that will grab lots of sessions in one go, and then close them all, while tracking
     * it's current state - is it currently able to acquire complete groups of sessions, or are there errors occurring?
     * <p>
     * This can thus be used to judge the state of the driver - is it currently healthy or not?
     */
    private static class SessionGrabber implements Runnable {
        private final Driver driver;
        private final CountDownLatch stopped = new CountDownLatch(1);
        private volatile boolean sessionsAreAvailable = false;
        private volatile boolean run = true;
        private volatile Throwable lastExceptionFromDriver;
        private final int sleepTimeout = 100;

        SessionGrabber(Driver driver) {
            this.driver = driver;
        }

        public void start() {
            new Thread(this).start();
        }

        @Override
        @SuppressWarnings("BusyWait")
        public void run() {
            try {
                while (run) {
                    try {
                        // Try and launch 8 concurrent sessions
                        startAndCloseTransactions(driver, 8);

                        // Success! We created 8 sessions without failures
                        sessionsAreAvailable = true;
                    } catch (Throwable e) {
                        lastExceptionFromDriver = e;
                        sessionsAreAvailable = false;
                    }
                    try {
                        Thread.sleep(sleepTimeout);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                stopped.countDown();
            }
        }

        @SuppressWarnings({"BusyWait", "CallToPrintStackTrace"})
        void assertSessionsAvailableWithin() throws InterruptedException {
            var deadline = System.currentTimeMillis() + 1000 * 120;
            while (System.currentTimeMillis() < deadline) {
                if (sessionsAreAvailable) {
                    // Success!
                    return;
                }
                Thread.sleep(sleepTimeout);
            }

            // Failure - timeout :(
            lastExceptionFromDriver.printStackTrace();
            fail("sessions did not become available from the driver after the db restart within the specified "
                    + "timeout. Last failure was: " + lastExceptionFromDriver.getMessage());
        }

        @SuppressWarnings("ResultOfMethodCallIgnored")
        public void stop() throws InterruptedException {
            run = false;
            stopped.await(10, SECONDS);
        }
    }
}
