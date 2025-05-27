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
package org.neo4j.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.RevocationCheckingStrategy.NO_CHECKS;
import static org.neo4j.driver.RevocationCheckingStrategy.STRICT;
import static org.neo4j.driver.RevocationCheckingStrategy.VERIFY_IF_PRESENT;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;
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
    void shouldDefaultToKnownCerts() {
        // Given
        var config = Config.defaultConfig();

        // When
        var authConfig = config.trustStrategy();

        // Then
        assertEquals(authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES);
    }

    @Test
    void shouldChangeToTrustedCert() {
        // Given
        var trustedCert = new File("trusted_cert");
        var config = Config.builder()
                .withTrustStrategy(Config.TrustStrategy.trustCustomCertificateSignedBy(trustedCert))
                .build();

        // When
        var authConfig = config.trustStrategy();

        // Then
        assertEquals(authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES);
        assertEquals(
                trustedCert.getAbsolutePath(), authConfig.certFiles().get(0).getAbsolutePath());
    }

    @Test
    void shouldSupportLivenessCheckTimeoutSetting() {
        var config = Config.builder()
                .withConnectionLivenessCheckTimeout(42, TimeUnit.SECONDS)
                .build();

        assertEquals(TimeUnit.SECONDS.toMillis(42), config.idleTimeBeforeConnectionTest());
    }

    @Test
    void shouldAllowZeroConnectionLivenessCheckTimeout() {
        var config = Config.builder()
                .withConnectionLivenessCheckTimeout(0, TimeUnit.SECONDS)
                .build();

        assertEquals(0, config.idleTimeBeforeConnectionTest());
    }

    @Test
    void shouldAllowNegativeConnectionLivenessCheckTimeout() {
        var config = Config.builder()
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
        var config =
                Config.builder().withMaxConnectionLifetime(42, TimeUnit.SECONDS).build();

        assertEquals(TimeUnit.SECONDS.toMillis(42), config.maxConnectionLifetimeMillis());
    }

    @Test
    void shouldAllowZeroConnectionMaxConnectionLifetime() {
        var config =
                Config.builder().withMaxConnectionLifetime(0, TimeUnit.SECONDS).build();

        assertEquals(0, config.maxConnectionLifetimeMillis());
    }

    @Test
    void shouldAllowNegativeConnectionMaxConnectionLifetime() {
        var config = Config.builder()
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
        var defaultConfig = Config.defaultConfig();
        assertEquals(TimeUnit.SECONDS.toMillis(30), defaultConfig.connectionTimeoutMillis());
    }

    @Test
    void shouldRespectConfiguredConnectionTimeout() {
        var config = Config.builder().withConnectionTimeout(42, TimeUnit.HOURS).build();
        assertEquals(TimeUnit.HOURS.toMillis(42), config.connectionTimeoutMillis());
    }

    @Test
    void shouldAllowConnectionTimeoutOfZero() {
        var config = Config.builder().withConnectionTimeout(0, TimeUnit.SECONDS).build();
        assertEquals(0, config.connectionTimeoutMillis());
    }

    @Test
    void shouldThrowForNegativeConnectionTimeout() {
        var builder = Config.builder();

        assertThrows(IllegalArgumentException.class, () -> builder.withConnectionTimeout(-42, TimeUnit.SECONDS));
    }

    @Test
    void shouldThrowForTooLargeConnectionTimeout() {
        var builder = Config.builder();

        assertThrows(
                IllegalArgumentException.class,
                () -> builder.withConnectionTimeout(Long.MAX_VALUE - 42, TimeUnit.SECONDS));
    }

    @Test
    void shouldNotAllowNegativeMaxRetryTimeMs() {
        var builder = Config.builder();

        assertThrows(IllegalArgumentException.class, () -> builder.withMaxTransactionRetryTime(-42, TimeUnit.SECONDS));
    }

    @Test
    void shouldAllowZeroMaxRetryTimeMs() {
        var config = Config.builder()
                .withMaxTransactionRetryTime(0, TimeUnit.SECONDS)
                .build();

        assertEquals(0, config.maxTransactionRetryTimeMillis());
    }

    @Test
    void shouldAllowPositiveRetryAttempts() {
        var config = Config.builder()
                .withMaxTransactionRetryTime(42, TimeUnit.SECONDS)
                .build();

        assertEquals(TimeUnit.SECONDS.toMillis(42), config.maxTransactionRetryTimeMillis());
    }

    @Test
    void shouldHaveCorrectDefaultMaxConnectionPoolSize() {
        assertEquals(100, Config.defaultConfig().maxConnectionPoolSize());
    }

    @Test
    void shouldAllowPositiveMaxConnectionPoolSize() {
        var config = Config.builder().withMaxConnectionPoolSize(42).build();

        assertEquals(42, config.maxConnectionPoolSize());
    }

    @Test
    void shouldAllowNegativeMaxConnectionPoolSize() {
        var config = Config.builder().withMaxConnectionPoolSize(-42).build();

        assertEquals(Integer.MAX_VALUE, config.maxConnectionPoolSize());
    }

    @Test
    void shouldDisallowZeroMaxConnectionPoolSize() {
        var e = assertThrows(
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
        var config = Config.builder()
                .withConnectionAcquisitionTimeout(42, TimeUnit.SECONDS)
                .build();

        assertEquals(TimeUnit.SECONDS.toMillis(42), config.connectionAcquisitionTimeoutMillis());
    }

    @Test
    void shouldAllowNegativeConnectionAcquisitionTimeout() {
        var config = Config.builder()
                .withConnectionAcquisitionTimeout(-42, TimeUnit.HOURS)
                .build();

        assertEquals(-1, config.connectionAcquisitionTimeoutMillis());
    }

    @Test
    void shouldAllowConnectionAcquisitionTimeoutOfZero() {
        var config = Config.builder()
                .withConnectionAcquisitionTimeout(0, TimeUnit.DAYS)
                .build();

        assertEquals(0, config.connectionAcquisitionTimeoutMillis());
    }

    @Test
    void shouldEnableAndDisableHostnameVerificationOnTrustStrategy() {
        var trustStrategy = Config.TrustStrategy.trustSystemCertificates();
        assertTrue(trustStrategy.isHostnameVerificationEnabled());

        assertSame(trustStrategy, trustStrategy.withHostnameVerification());
        assertTrue(trustStrategy.isHostnameVerificationEnabled());

        assertSame(trustStrategy, trustStrategy.withoutHostnameVerification());
        assertFalse(trustStrategy.isHostnameVerificationEnabled());
    }

    @Test
    void shouldEnableAndDisableCertificateRevocationChecksOnTestStrategy() {
        var trustStrategy = Config.TrustStrategy.trustSystemCertificates();
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
        var resolver = mock(ServerAddressResolver.class);
        var config = Config.builder().withResolver(resolver).build();

        assertEquals(resolver, config.resolver());
    }

    @Test
    void shouldNotAllowNullResolver() {
        assertThrows(NullPointerException.class, () -> Config.builder().withResolver(null));
    }

    @Test
    void shouldDefaultToDefaultFetchSize() {
        var config = Config.defaultConfig();
        assertEquals(1000, config.fetchSize());
    }

    @ParameterizedTest
    @ValueSource(longs = {100, 1, 1000, Long.MAX_VALUE, -1})
    void shouldChangeFetchSize(long value) {
        var config = Config.builder().withFetchSize(value).build();
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
        var config = Config.builder().withEventLoopThreads(value).build();
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
        var config = Config.builder().withUserAgent("AwesomeDriver").build();
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
        var config = Config.builder().build();
        var metricsAdapter = config.metricsAdapter();

        assertEquals(MetricsAdapter.DEV_NULL, metricsAdapter);
        assertFalse(config.isMetricsEnabled());
    }

    @Test
    void shouldNotAcceptNullMeterRegistry() {
        var builder = Config.builder();
        assertThrows(NullPointerException.class, () -> builder.withMetricsAdapter(null));
    }

    @Test
    void shouldSetMetricsAdapter() {
        var config = Config.builder().withMetricsAdapter(MetricsAdapter.DEFAULT).build();
        var metricsAdapter = config.metricsAdapter();

        assertEquals(MetricsAdapter.DEFAULT, metricsAdapter);
        assertTrue(config.isMetricsEnabled());
    }

    @Test
    void shouldSetRoutingTablePurgeDelayMillis() {
        // GIVEN
        var delay = 1000L;

        // WHEN
        var config = Config.builder()
                .withRoutingTablePurgeDelay(delay, TimeUnit.MILLISECONDS)
                .build();

        // THEN
        assertEquals(delay, config.routingTablePurgeDelayMillis());
    }

    @Test
    void shouldMaxTransactionRetryTimeMillis() {
        // GIVEN
        var retryTime = 1000L;

        // WHEN
        var config = Config.builder()
                .withMaxTransactionRetryTime(retryTime, TimeUnit.MILLISECONDS)
                .build();

        // THEN
        assertEquals(retryTime, config.maxTransactionRetryTimeMillis());
    }

    @Nested
    class SerializationTest {
        @SuppressWarnings("deprecation")
        @Test
        void shouldSerialize() throws Exception {
            var config = Config.builder()
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
                    .withNotificationConfig(NotificationConfig.defaultConfig()
                            .enableMinimumSeverity(NotificationSeverity.WARNING)
                            .disableCategories(
                                    Set.of(NotificationCategory.UNSUPPORTED, NotificationCategory.UNRECOGNIZED)))
                    .build();

            var verify = TestUtil.serializeAndReadBack(config, Config.class);

            assertEquals(config.maxConnectionPoolSize(), verify.maxConnectionPoolSize());
            assertEquals(config.connectionTimeoutMillis(), verify.connectionTimeoutMillis());
            assertEquals(config.connectionAcquisitionTimeoutMillis(), verify.connectionAcquisitionTimeoutMillis());
            assertEquals(config.idleTimeBeforeConnectionTest(), verify.idleTimeBeforeConnectionTest());
            assertEquals(config.maxConnectionLifetimeMillis(), verify.maxConnectionLifetimeMillis());
            assertEquals(Logging.systemLogging().getClass(), verify.logging().getClass());
            assertEquals(config.maxTransactionRetryTimeMillis(), verify.maxTransactionRetryTimeMillis());
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
            assertEquals(config.maxTransactionRetryTimeMillis(), verify.maxTransactionRetryTimeMillis());
            assertEquals(config.logLeakedSessions(), verify.logLeakedSessions());
            assertEquals(
                    NotificationConfig.defaultConfig()
                            .enableMinimumSeverity(NotificationSeverity.WARNING)
                            .disableCategories(
                                    Set.of(NotificationCategory.UNSUPPORTED, NotificationCategory.UNRECOGNIZED)),
                    config.notificationConfig());
            assertEquals(config.isTelemetryDisabled(), verify.isTelemetryDisabled());
        }

        @Test
        void shouldSerializeSerializableLogging() throws IOException, ClassNotFoundException {
            @SuppressWarnings("deprecation")
            var config = Config.builder()
                    .withLogging(Logging.javaUtilLogging(Level.ALL))
                    .build();

            var verify = TestUtil.serializeAndReadBack(config, Config.class);
            @SuppressWarnings("deprecation")
            var logging = verify.logging();
            assertInstanceOf(JULogging.class, logging);

            var loggingLevelFields = ReflectionSupport.findFields(
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
        void officialLoggingProvidersShouldBeSerializable(
                @SuppressWarnings("deprecation") Class<? extends Logging> loggingClass) {
            assertTrue(Serializable.class.isAssignableFrom(loggingClass));
        }
    }

    @Test
    void shouldHaveDefaultUserAgent() {
        var config = Config.defaultConfig();

        assertTrue(config.userAgent().matches("^neo4j-java/.+$"));
    }

    @Test
    void shouldDefaultToTelemetryEnabled() {
        // Given
        var config = Config.defaultConfig();

        // When
        var telemetryDisabled = config.isTelemetryDisabled();

        // Then
        assertFalse(telemetryDisabled);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldChangeTelemetryDisabled(boolean disabled) {
        // Given
        var config = Config.builder().withTelemetryDisabled(disabled).build();

        // When
        var telemetryDisabled = config.isTelemetryDisabled();

        // Then
        assertEquals(disabled, telemetryDisabled);
    }

    @Test
    void shouldNotHaveMinimumNotificationSeverity() {
        var config = Config.defaultConfig();

        assertTrue(config.minimumNotificationSeverity().isEmpty());
    }

    @Test
    void shouldSetMinimumNotificationSeverity() {
        var config = Config.builder()
                .withMinimumNotificationSeverity(NotificationSeverity.WARNING)
                .build();

        assertEquals(
                NotificationSeverity.WARNING,
                config.minimumNotificationSeverity().orElse(null));
    }

    @Test
    void shouldNotHaveDisabledNotificationClassifications() {
        var config = Config.defaultConfig();

        assertTrue(config.disabledNotificationClassifications().isEmpty());
    }

    @Test
    void shouldSetDisabledNotificationClassifications() {
        var config = Config.builder()
                .withDisabledNotificationClassifications(Set.of(NotificationClassification.SECURITY))
                .build();

        assertEquals(Set.of(NotificationClassification.SECURITY), config.disabledNotificationClassifications());
    }
}
