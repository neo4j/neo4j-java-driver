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
package org.neo4j.driver.internal;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.Config;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.observation.NoopObservationProvider;
import org.neo4j.driver.internal.security.BoltSecurityPlanManager;
import org.neo4j.driver.property_encryption.BasePropertyEncryption;
import org.neo4j.driver.property_encryption.PropertyEncryption;
import org.neo4j.driver.property_encryption.async.AsyncPropertyEncryption;
import org.neo4j.driver.property_encryption.reactive.ReactivePropertyEncryption;

class InternalDriverTest {
    @Test
    void shouldCloseSessionFactory() {
        var sessionFactory = sessionFactoryMock();
        var driver = newDriver(sessionFactory);

        assertNull(await(driver.closeAsync()));
        verify(sessionFactory).close();
    }

    @Test
    void shouldNotCloseSessionFactoryMultipleTimes() {
        var sessionFactory = sessionFactoryMock();
        var driver = newDriver(sessionFactory);

        assertNull(await(driver.closeAsync()));
        assertNull(await(driver.closeAsync()));
        assertNull(await(driver.closeAsync()));

        verify(sessionFactory).close();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldVerifyConnectivity() {
        var sessionFactory = sessionFactoryMock();
        CompletableFuture<Void> connectivityStage = completedWithNull();
        when(sessionFactory.verifyConnectivity()).thenReturn(connectivityStage);

        var driver = newDriver(sessionFactory);

        assertEquals(connectivityStage, driver.verifyConnectivityAsync());
    }

    @Test
    @SuppressWarnings("resource")
    void shouldThrowWhenUnableToVerifyConnectivity() {
        var sessionFactory = mock(SessionFactory.class);
        var error = new ServiceUnavailableException("Hello");
        when(sessionFactory.verifyConnectivity()).thenReturn(failedFuture(error));
        var driver = newDriver(sessionFactory);

        var e = assertThrows(ServiceUnavailableException.class, () -> await(driver.verifyConnectivityAsync()));
        assertEquals(e.getMessage(), "Hello");
    }

    @Test
    void shouldCreateExecutableQuery() {
        // Given
        var driver = newDriver();
        var query = "string";

        // When
        var executableQuery = (InternalExecutableQuery) driver.executableQuery(query);

        // Then
        assertNotNull(executableQuery);
        assertEquals(driver, executableQuery.driver());
        assertEquals(query, executableQuery.query());
        assertEquals(Collections.emptyMap(), executableQuery.parameters());
        assertEquals(QueryConfig.defaultConfig(), executableQuery.config());
    }

    @Test
    void shouldReturnPropertyEncryption() {
        // Given
        var driver = newDriver();

        // When
        var propertyEncryption = driver.propertyEncryption();

        // Then
        assertNotNull(propertyEncryption);
        assertInstanceOf(PropertyEncryption.class, propertyEncryption);
    }

    @ParameterizedTest
    @ValueSource(
            classes = {
                PropertyEncryption.class,
                AsyncPropertyEncryption.class,
                ReactivePropertyEncryption.class,
                org.neo4j.driver.property_encryption.reactivestreams.ReactivePropertyEncryption.class
            })
    <T extends BasePropertyEncryption> void shouldReturnPropertyEncryption(Class<T> propertyEncryptionClass) {
        // Given
        var driver = newDriver();

        // When
        var propertyEncryption = driver.propertyEncryption(propertyEncryptionClass);

        // Then
        assertNotNull(propertyEncryption);
        assertInstanceOf(propertyEncryptionClass, propertyEncryption);
    }

    private static InternalDriver newDriver(SessionFactory sessionFactory) {
        return new InternalDriver(
                BoltSecurityPlanManager.insecure(),
                sessionFactory,
                true,
                DEV_NULL_LOGGING,
                NoopObservationProvider.getInstance(),
                Map.of(),
                mock());
    }

    private static SessionFactory sessionFactoryMock() {
        var sessionFactory = mock(SessionFactory.class);
        when(sessionFactory.close()).thenReturn(completedWithNull());
        return sessionFactory;
    }

    private static InternalDriver newDriver() {
        var sessionFactory = sessionFactoryMock();
        var config = Config.defaultConfig();

        return new InternalDriver(
                BoltSecurityPlanManager.insecure(),
                sessionFactory,
                true,
                DEV_NULL_LOGGING,
                NoopObservationProvider.getInstance(),
                Map.of(),
                mock());
    }
}
