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
package org.neo4j.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.RevocationCheckingStrategy.NO_CHECKS;
import static org.neo4j.driver.RevocationCheckingStrategy.STRICT;
import static org.neo4j.driver.RevocationCheckingStrategy.VERIFY_IF_PRESENT;
import static org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil.DEFAULT_FETCH_SIZE;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.support.ReflectionSupport;
import org.neo4j.driver.internal.logging.ConsoleLogging;
import org.neo4j.driver.internal.logging.DevNullLogging;
import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.internal.logging.Slf4jLogging;
import org.neo4j.driver.net.ServerAddressResolver;
import org.neo4j.driver.testutil.TestUtil;

class ConfigTest {
    @Test
    void shouldReturnDefaultBookmarkManager() {
        // Given
        var config = Config.defaultConfig();

        // When
        var manager = config.queryBookmarkManager();

        // Then
        assertEquals(
                BookmarkManagers.defaultManager(BookmarkManagerConfig.builder().build())
                        .getClass(),
                manager.getClass());
    }

    @Test
    void shouldUpdateBookmarkManager() {
        // Given
        var manager = mock(BookmarkManager.class);

        // When
        var config = Config.builder().withQueryBookmarkManager(manager).build();

        // Then
        assertEquals(manager, config.queryBookmarkManager());
    }

    @Test
    void shouldNotAllowNullBookmarkManager() {
        assertThrows(NullPointerException.class, () -> Config.builder().withQueryBookmarkManager(null));
    }

    @Test
    void shouldDefaultToKnownCerts() {
        // Given
        Config config = Config.defaultConfig();

        // When
        Config.TrustStrategy authConfig = config.trustStrategy();

        // Then
        assertEquals(authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES);
    }

    @Test
    void shouldChangeToTrustedCert() {
        // Given
        File trustedCert = new File("trusted_cert");
        Config config = Config.builder()
                .withTrustStrategy(Config.TrustStrategy.trustCustomCertificateSignedBy(trustedCert))
                .build();

        // When
        Config.TrustStrategy authConfig = config.trustStrategy();

        // Then
        assertEquals(authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES);
        assertEquals(
                trustedCert.getAbsolutePath(), authConfig.certFiles().get(0).getAbsolutePath());
    }

    @Test
    void shouldSupportLivenessCheckTimeoutSetting() {
        Config config = Config.builder()
                .withConnectionLivenessCheckTimeout(42, TimeUnit.SECONDS)
                .build();

        assertEquals(TimeUnit.SECONDS.toMillis(42), config.idleTimeBeforeConnectionTest());
    }

    @Test
    void shouldAllowZeroConnectionLivenessCheckTimeout() {
        Config config = Config.builder()
                .withConnectionLivenessCheckTimeout(0, TimeUnit.SECONDS)
                .build();

        assertEquals(0, config.idleTimeBeforeConnectionTest());
    }

    @Test
    void shouldAllowNegativeConnectionLivenessCheckTimeout() {
        Config config = Config.builder()
                .withConnectionLivenessCheckTimeout(-42, TimeUnit.SECONDS)
                .build();

        assertEquals(TimeUnit.SECONDS.toMillis(-42), config.idleTimeBeforeConnectionTest());
    }

    @Test
    void shouldHaveCorrectMaxConnectionLifetime() {
        assertEquals(TimeUnit.HOURS.toMillis(1), Config.defaultConfig().maxConnectionLifetimeMillis());
    }

    @Test
    void shouldSupportMaxConnectionLifetimeSetting() {
        Config config =
                Config.builder().withMaxConnectionLifetime(42, TimeUnit.SECONDS).build();

        assertEquals(TimeUnit.SECONDS.toMillis(42), config.maxConnectionLifetimeMillis());
    }

    @Test
    void shouldAllowZeroConnectionMaxConnectionLifetime() {
        Config config =
                Config.builder().withMaxConnectionLifetime(0, TimeUnit.SECONDS).build();

        assertEquals(0, config.maxConnectionLifetimeMillis());
    }

    @Test
    void shouldAllowNegativeConnectionMaxConnectionLifetime() {
        Config config = Config.builder()
                .withMaxConnectionLifetime(-42, TimeUnit.SECONDS)
                .build();

        assertEquals(TimeUnit.SECONDS.toMillis(-42), config.maxConnectionLifetimeMillis());
    }

    @Test
    void shouldTurnOnLeakedSessionsLogging() {
        // leaked sessions logging is turned off by default
        assertFalse(Config.builder().build().logLeakedSessions());

        // it can be turned on using config
        assertTrue(Config.builder().withLeakedSessionsLogging().build().logLeakedSessions());
    }

    @Test
    void shouldHaveDefaultConnectionTimeout() {
        Config defaultConfig = Config.defaultConfig();
        assertEquals(TimeUnit.SECONDS.toMillis(30), defaultConfig.connectionTimeoutMillis());
    }

    @Test
    void shouldRespectConfiguredConnectionTimeout() {
        Config config =
                Config.builder().withConnectionTimeout(42, TimeUnit.HOURS).build();
        assertEquals(TimeUnit.HOURS.toMillis(42), config.connectionTimeoutMillis());
    }

    @Test
    void shouldAllowConnectionTimeoutOfZero() {
        Config config =
                Config.builder().withConnectionTimeout(0, TimeUnit.SECONDS).build();
        assertEquals(0, config.connectionTimeoutMillis());
    }

    @Test
    void shouldThrowForNegativeConnectionTimeout() {
        Config.ConfigBuilder builder = Config.builder();

        assertThrows(IllegalArgumentException.class, () -> builder.withConnectionTimeout(-42, TimeUnit.SECONDS));
    }

    @Test
    void shouldThrowForTooLargeConnectionTimeout() {
        Config.ConfigBuilder builder = Config.builder();

        assertThrows(
                IllegalArgumentException.class,
                () -> builder.withConnectionTimeout(Long.MAX_VALUE - 42, TimeUnit.SECONDS));
    }

    @Test
    void shouldNotAllowNegativeMaxRetryTimeMs() {
        Config.ConfigBuilder builder = Config.builder();

        assertThrows(IllegalArgumentException.class, () -> builder.withMaxTransactionRetryTime(-42, TimeUnit.SECONDS));
    }

    @Test
    void shouldAllowZeroMaxRetryTimeMs() {
        Config config = Config.builder()
                .withMaxTransactionRetryTime(0, TimeUnit.SECONDS)
                .build();

        assertEquals(0, config.retrySettings().maxRetryTimeMs());
    }

    @Test
    void shouldAllowPositiveRetryAttempts() {
        Config config = Config.builder()
                .withMaxTransactionRetryTime(42, TimeUnit.SECONDS)
                .build();

        assertEquals(TimeUnit.SECONDS.toMillis(42), config.retrySettings().maxRetryTimeMs());
    }

    @Test
    void shouldHaveCorrectDefaultMaxConnectionPoolSize() {
        assertEquals(100, Config.defaultConfig().maxConnectionPoolSize());
    }

    @Test
    void shouldAllowPositiveMaxConnectionPoolSize() {
        Config config = Config.builder().withMaxConnectionPoolSize(42).build();

        assertEquals(42, config.maxConnectionPoolSize());
    }

    @Test
    void shouldAllowNegativeMaxConnectionPoolSize() {
        Config config = Config.builder().withMaxConnectionPoolSize(-42).build();

        assertEquals(Integer.MAX_VALUE, config.maxConnectionPoolSize());
    }

    @Test
    void shouldDisallowZeroMaxConnectionPoolSize() {
        IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> Config.builder().withMaxConnectionPoolSize(0).build());
        assertEquals("Zero value is not supported", e.getMessage());
    }

    @Test
    void shouldHaveCorrectDefaultConnectionAcquisitionTimeout() {
        assertEquals(TimeUnit.SECONDS.toMillis(60), Config.defaultConfig().connectionAcquisitionTimeoutMillis());
    }

    @Test
    void shouldAllowPositiveConnectionAcquisitionTimeout() {
        Config config = Config.builder()
                .withConnectionAcquisitionTimeout(42, TimeUnit.SECONDS)
                .build();

        assertEquals(TimeUnit.SECONDS.toMillis(42), config.connectionAcquisitionTimeoutMillis());
    }

    @Test
    void shouldAllowNegativeConnectionAcquisitionTimeout() {
        Config config = Config.builder()
                .withConnectionAcquisitionTimeout(-42, TimeUnit.HOURS)
                .build();

        assertEquals(-1, config.connectionAcquisitionTimeoutMillis());
    }

    @Test
    void shouldAllowConnectionAcquisitionTimeoutOfZero() {
        Config config = Config.builder()
                .withConnectionAcquisitionTimeout(0, TimeUnit.DAYS)
                .build();

        assertEquals(0, config.connectionAcquisitionTimeoutMillis());
    }

    @Test
    void shouldEnableAndDisableHostnameVerificationOnTrustStrategy() {
        Config.TrustStrategy trustStrategy = Config.TrustStrategy.trustSystemCertificates();
        assertTrue(trustStrategy.isHostnameVerificationEnabled());

        assertSame(trustStrategy, trustStrategy.withHostnameVerification());
        assertTrue(trustStrategy.isHostnameVerificationEnabled());

        assertSame(trustStrategy, trustStrategy.withoutHostnameVerification());
        assertFalse(trustStrategy.isHostnameVerificationEnabled());
    }

    @Test
    void shouldEnableAndDisableCertificateRevocationChecksOnTestStrategy() {
        Config.TrustStrategy trustStrategy = Config.TrustStrategy.trustSystemCertificates();
        assertEquals(NO_CHECKS, trustStrategy.revocationCheckingStrategy());

        assertSame(trustStrategy, trustStrategy.withoutCertificateRevocationChecks());
        assertEquals(NO_CHECKS, trustStrategy.revocationCheckingStrategy());

        assertSame(trustStrategy, trustStrategy.withStrictRevocationChecks());
        assertEquals(STRICT, trustStrategy.revocationCheckingStrategy());

        assertSame(trustStrategy, trustStrategy.withVerifyIfPresentRevocationChecks());
        assertEquals(VERIFY_IF_PRESENT, trustStrategy.revocationCheckingStrategy());
    }

    @Test
    void shouldAllowToConfigureResolver() {
        ServerAddressResolver resolver = mock(ServerAddressResolver.class);
        Config config = Config.builder().withResolver(resolver).build();

        assertEquals(resolver, config.resolver());
    }

    @Test
    void shouldNotAllowNullResolver() {
        assertThrows(NullPointerException.class, () -> Config.builder().withResolver(null));
    }

    @Test
    void shouldDefaultToDefaultFetchSize() {
        Config config = Config.defaultConfig();
        assertEquals(DEFAULT_FETCH_SIZE, config.fetchSize());
    }

    @ParameterizedTest
    @ValueSource(longs = {100, 1, 1000, Long.MAX_VALUE, -1})
    void shouldChangeFetchSize(long value) {
        Config config = Config.builder().withFetchSize(value).build();
        assertEquals(value, config.fetchSize());
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -100, -2})
    void shouldErrorWithIllegalFetchSize(long value) {
        assertThrows(
                IllegalArgumentException.class,
                () -> Config.builder().withFetchSize(value).build());
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 1, 1000, Integer.MAX_VALUE})
    void shouldChangeEventLoopThreads(int value) {
        Config config = Config.builder().withEventLoopThreads(value).build();
        assertEquals(value, config.eventLoopThreads());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -100, -2})
    void shouldErrorWithIllegalEventLoopThreadsSize(int value) {
        assertThrows(
                IllegalArgumentException.class,
                () -> Config.builder().withEventLoopThreads(value).build());
    }

    @Test
    void shouldChangeUserAgent() {
        Config config = Config.builder().withUserAgent("AwesomeDriver").build();
        assertEquals("AwesomeDriver", config.userAgent());
    }

    @Test
    void shouldErrorWithInvalidUserAgent() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Config.builder().withUserAgent(null).build());
        assertThrows(
                IllegalArgumentException.class,
                () -> Config.builder().withUserAgent("").build());
    }

    @Test
    void shouldNotHaveMeterRegistryByDefault() {
        Config config = Config.builder().build();
        MetricsAdapter metricsAdapter = config.metricsAdapter();

        assertEquals(MetricsAdapter.DEV_NULL, metricsAdapter);
        assertFalse(config.isMetricsEnabled());
    }

    @Test
    void shouldNotAcceptNullMeterRegistry() {
        Config.ConfigBuilder builder = Config.builder();
        assertThrows(NullPointerException.class, () -> builder.withMetricsAdapter(null));
    }

    @Test
    void shouldSetMetricsAdapter() {
        Config config =
                Config.builder().withMetricsAdapter(MetricsAdapter.DEFAULT).build();
        MetricsAdapter metricsAdapter = config.metricsAdapter();

        assertEquals(MetricsAdapter.DEFAULT, metricsAdapter);
        assertTrue(config.isMetricsEnabled());
    }

    @Nested
    class SerializationTest {
        @Test
        void shouldSerialize() throws Exception {
            Config config = Config.builder()
                    .withMaxConnectionPoolSize(123)
                    .withConnectionTimeout(6543L, TimeUnit.MILLISECONDS)
                    .withConnectionAcquisitionTimeout(5432L, TimeUnit.MILLISECONDS)
                    .withConnectionLivenessCheckTimeout(4321L, TimeUnit.MILLISECONDS)
                    .withMaxConnectionLifetime(4711, TimeUnit.MILLISECONDS)
                    .withMaxTransactionRetryTime(3210L, TimeUnit.MILLISECONDS)
                    .withFetchSize(9876L)
                    .withEventLoopThreads(4)
                    .withoutEncryption()
                    .withTrustStrategy(Config.TrustStrategy.trustCustomCertificateSignedBy(new File("doesntMatter")))
                    .withUserAgent("user-agent")
                    .withDriverMetrics()
                    .withRoutingTablePurgeDelay(50000, TimeUnit.MILLISECONDS)
                    .withLeakedSessionsLogging()
                    .withMetricsAdapter(MetricsAdapter.MICROMETER)
                    .build();

            Config verify = TestUtil.serializeAndReadBack(config, Config.class);

            assertEquals(config.maxConnectionPoolSize(), verify.maxConnectionPoolSize());
            assertEquals(config.connectionTimeoutMillis(), verify.connectionTimeoutMillis());
            assertEquals(config.connectionAcquisitionTimeoutMillis(), verify.connectionAcquisitionTimeoutMillis());
            assertEquals(config.idleTimeBeforeConnectionTest(), verify.idleTimeBeforeConnectionTest());
            assertEquals(config.maxConnectionLifetimeMillis(), verify.maxConnectionLifetimeMillis());
            assertNotNull(verify.retrySettings());
            assertSame(DevNullLogging.DEV_NULL_LOGGING, verify.logging());
            assertEquals(
                    config.retrySettings().maxRetryTimeMs(),
                    verify.retrySettings().maxRetryTimeMs());
            assertEquals(config.fetchSize(), verify.fetchSize());
            assertEquals(config.eventLoopThreads(), verify.eventLoopThreads());
            assertEquals(config.encrypted(), verify.encrypted());
            assertEquals(
                    config.trustStrategy().strategy(), verify.trustStrategy().strategy());
            assertEquals(
                    config.trustStrategy().certFiles(), verify.trustStrategy().certFiles());
            assertEquals(
                    config.trustStrategy().isHostnameVerificationEnabled(),
                    verify.trustStrategy().isHostnameVerificationEnabled());
            assertEquals(
                    config.trustStrategy().revocationCheckingStrategy(),
                    verify.trustStrategy().revocationCheckingStrategy());
            assertEquals(config.userAgent(), verify.userAgent());
            assertEquals(config.isMetricsEnabled(), verify.isMetricsEnabled());
            assertEquals(config.metricsAdapter(), verify.metricsAdapter());
            assertEquals(
                    config.routingSettings().routingTablePurgeDelayMs(),
                    verify.routingSettings().routingTablePurgeDelayMs());
            assertEquals(config.logLeakedSessions(), verify.logLeakedSessions());
        }

        @Test
        void shouldSerializeSerializableLogging() throws IOException, ClassNotFoundException {
            Config config = Config.builder()
                    .withLogging(Logging.javaUtilLogging(Level.ALL))
                    .build();

            Config verify = TestUtil.serializeAndReadBack(config, Config.class);
            Logging logging = verify.logging();
            assertInstanceOf(JULogging.class, logging);

            List<Field> loggingLevelFields = ReflectionSupport.findFields(
                    JULogging.class, f -> "loggingLevel".equals(f.getName()), HierarchyTraversalMode.TOP_DOWN);
            assertFalse(loggingLevelFields.isEmpty());
            loggingLevelFields.forEach(field -> {
                try {
                    field.setAccessible(true);
                    assertEquals(Level.ALL, field.get(logging));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @ParameterizedTest
        @ValueSource(classes = {DevNullLogging.class, JULogging.class, ConsoleLogging.class, Slf4jLogging.class})
        void officialLoggingProvidersShouldBeSerializable(Class<? extends Logging> loggingClass) {
            assertTrue(Serializable.class.isAssignableFrom(loggingClass));
        }
    }
}
