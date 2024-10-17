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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.Config.TrustStrategy.trustCustomCertificateSignedBy;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.security.BoltSecurityPlanManager;
import org.neo4j.driver.internal.util.DriverFactoryWithClock;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

/**
 * Mainly concerned about the connection pool - we want to make sure that bad connections are evacuated from the
 * pool properly if the server dies, or all connections are lost for some other reason.
 */
@ParallelizableIT
class ServerKilledIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of("plaintext", Config.builder().withoutEncryption()),
                Arguments.of(
                        "tls encrypted",
                        Config.builder()
                                .withEncryption()
                                .withTrustStrategy(trustCustomCertificateSignedBy(neo4j.tlsCertFile()))));
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldRecoverFromServerRestart(String name, Config.ConfigBuilder configBuilder) {
        // Given config with sessionLivenessCheckTimeout not set, i.e. turned off
        try (var driver = GraphDatabase.driver(neo4j.uri(), neo4j.authTokenManager(), configBuilder.build())) {
            acquireAndReleaseConnections(4, driver);

            // When
            neo4j.stopProxy();
            neo4j.startProxy();

            // Then we should be able to start using sessions again, at most O(numSessions) session calls later
            var toleratedFailures = 4;
            for (var i = 0; i < 10; i++) {
                try (var s = driver.session()) {
                    s.run("RETURN 'Hello, world!'");
                } catch (ServiceUnavailableException e) {
                    if (toleratedFailures-- == 0) {
                        fail("Expected (for now) at most four failures, one for each old connection, but now I've "
                                + "gotten " + "five: " + e.getMessage());
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldDropBrokenOldSessions(String name, Config.ConfigBuilder configBuilder) {
        // config with set liveness check timeout
        var livenessCheckTimeoutMinutes = 10;
        configBuilder.withConnectionLivenessCheckTimeout(livenessCheckTimeoutMinutes, TimeUnit.MINUTES);

        var clock = new FakeClock();

        try (var driver = createDriver(clock, configBuilder.build())) {
            acquireAndReleaseConnections(5, driver);

            // restart database access to invalidate all idle connections in the pool
            neo4j.stopProxy();
            neo4j.startProxy();
            // move clock forward more than configured liveness check timeout
            clock.progress(TimeUnit.MINUTES.toMillis(livenessCheckTimeoutMinutes + 1));

            // now all idle connections should be considered too old and will be verified during acquisition
            // they will appear broken because of the database restart and new valid connection will be created
            try (var session = driver.session()) {
                var records = session.run("RETURN 1").list();
                assertEquals(1, records.size());
                assertEquals(1, records.get(0).get(0).asInt());
            }
        }
    }

    private static void acquireAndReleaseConnections(int count, Driver driver) {
        if (count > 0) {
            var session = driver.session();
            session.run("RETURN 1");
            acquireAndReleaseConnections(count - 1, driver);
            session.close();
        }
    }

    private Driver createDriver(Clock clock, Config config) {
        DriverFactory factory = new DriverFactoryWithClock(clock);
        return factory.newInstance(
                neo4j.uri(), neo4j.authTokenManager(), config, BoltSecurityPlanManager.insecure(), null, null);
    }
}
