/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.net.ServerAddressResolver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class ConfigTest
{
    @Test
    void shouldDefaultToKnownCerts()
    {
        // Given
        Config config = Config.defaultConfig();

        // When
        Config.TrustStrategy authConfig = config.trustStrategy();

        // Then
        assertEquals( authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES );
    }

    @Test
    void shouldChangeToTrustedCert()
    {
        // Given
        File trustedCert = new File( "trusted_cert" );
        Config config = Config.builder().withTrustStrategy( Config.TrustStrategy.trustCustomCertificateSignedBy( trustedCert ) ).build();

        // When
        Config.TrustStrategy authConfig = config.trustStrategy();

        // Then
        assertEquals( authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES );
        assertEquals( trustedCert.getAbsolutePath(), authConfig.certFile().getAbsolutePath() );
    }

    @Test
    void shouldSupportLivenessCheckTimeoutSetting() throws Throwable
    {
        Config config = Config.builder().withConnectionLivenessCheckTimeout( 42, TimeUnit.SECONDS ).build();

        assertEquals( TimeUnit.SECONDS.toMillis( 42 ), config.idleTimeBeforeConnectionTest() );
    }

    @Test
    void shouldAllowZeroConnectionLivenessCheckTimeout() throws Throwable
    {
        Config config = Config.builder().withConnectionLivenessCheckTimeout( 0, TimeUnit.SECONDS ).build();

        assertEquals( 0, config.idleTimeBeforeConnectionTest() );
    }

    @Test
    void shouldAllowNegativeConnectionLivenessCheckTimeout() throws Throwable
    {
        Config config = Config.builder().withConnectionLivenessCheckTimeout( -42, TimeUnit.SECONDS ).build();

        assertEquals( TimeUnit.SECONDS.toMillis( -42 ), config.idleTimeBeforeConnectionTest() );
    }

    @Test
    void shouldHaveCorrectMaxConnectionLifetime()
    {
        assertEquals( TimeUnit.HOURS.toMillis( 1 ), Config.defaultConfig().maxConnectionLifetimeMillis() );
    }

    @Test
    void shouldSupportMaxConnectionLifetimeSetting() throws Throwable
    {
        Config config = Config.builder().withMaxConnectionLifetime( 42, TimeUnit.SECONDS ).build();

        assertEquals( TimeUnit.SECONDS.toMillis( 42 ), config.maxConnectionLifetimeMillis() );
    }

    @Test
    void shouldAllowZeroConnectionMaxConnectionLifetime() throws Throwable
    {
        Config config = Config.builder().withMaxConnectionLifetime( 0, TimeUnit.SECONDS ).build();

        assertEquals( 0, config.maxConnectionLifetimeMillis() );
    }

    @Test
    void shouldAllowNegativeConnectionMaxConnectionLifetime() throws Throwable
    {
        Config config = Config.builder().withMaxConnectionLifetime( -42, TimeUnit.SECONDS ).build();

        assertEquals( TimeUnit.SECONDS.toMillis( -42 ), config.maxConnectionLifetimeMillis() );
    }

    @Test
    void shouldTurnOnLeakedSessionsLogging()
    {
        // leaked sessions logging is turned off by default
        assertFalse( Config.builder().build().logLeakedSessions() );

        // it can be turned on using config
        assertTrue( Config.builder().withLeakedSessionsLogging().build().logLeakedSessions() );
    }

    @Test
    void shouldHaveDefaultConnectionTimeout()
    {
        Config defaultConfig = Config.defaultConfig();
        assertEquals( TimeUnit.SECONDS.toMillis( 5 ), defaultConfig.connectionTimeoutMillis() );
    }

    @Test
    void shouldRespectConfiguredConnectionTimeout()
    {
        Config config = Config.builder().withConnectionTimeout( 42, TimeUnit.HOURS ).build();
        assertEquals( TimeUnit.HOURS.toMillis( 42 ), config.connectionTimeoutMillis() );
    }

    @Test
    void shouldAllowConnectionTimeoutOfZero()
    {
        Config config = Config.builder().withConnectionTimeout( 0, TimeUnit.SECONDS ).build();
        assertEquals( 0, config.connectionTimeoutMillis() );
    }

    @Test
    void shouldThrowForNegativeConnectionTimeout()
    {
        Config.ConfigBuilder builder = Config.builder();

        assertThrows( IllegalArgumentException.class, () -> builder.withConnectionTimeout( -42, TimeUnit.SECONDS ) );
    }

    @Test
    void shouldThrowForTooLargeConnectionTimeout()
    {
        Config.ConfigBuilder builder = Config.builder();

        assertThrows( IllegalArgumentException.class, () -> builder.withConnectionTimeout( Long.MAX_VALUE - 42, TimeUnit.SECONDS ) );
    }

    @Test
    void shouldNotAllowNegativeMaxRetryTimeMs()
    {
        Config.ConfigBuilder builder = Config.builder();

        assertThrows( IllegalArgumentException.class, () -> builder.withMaxTransactionRetryTime( -42, TimeUnit.SECONDS ) );
    }

    @Test
    void shouldAllowZeroMaxRetryTimeMs()
    {
        Config config = Config.builder().withMaxTransactionRetryTime( 0, TimeUnit.SECONDS ).build();

        assertEquals( 0, config.retrySettings().maxRetryTimeMs() );
    }

    @Test
    void shouldAllowPositiveRetryAttempts()
    {
        Config config = Config.builder().withMaxTransactionRetryTime( 42, TimeUnit.SECONDS ).build();

        assertEquals( TimeUnit.SECONDS.toMillis( 42 ), config.retrySettings().maxRetryTimeMs() );
    }

    @Test
    void shouldHaveCorrectDefaultMaxConnectionPoolSize()
    {
        assertEquals( 100, Config.defaultConfig().maxConnectionPoolSize() );
    }

    @Test
    void shouldAllowPositiveMaxConnectionPoolSize()
    {
        Config config = Config.builder().withMaxConnectionPoolSize( 42 ).build();

        assertEquals( 42, config.maxConnectionPoolSize() );
    }

    @Test
    void shouldAllowNegativeMaxConnectionPoolSize()
    {
        Config config = Config.builder().withMaxConnectionPoolSize( -42 ).build();

        assertEquals( Integer.MAX_VALUE, config.maxConnectionPoolSize() );
    }

    @Test
    void shouldDisallowZeroMaxConnectionPoolSize()
    {
        IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> Config.builder().withMaxConnectionPoolSize( 0 ).build() );
        assertEquals( "Zero value is not supported", e.getMessage() );
    }

    @Test
    void shouldHaveCorrectDefaultConnectionAcquisitionTimeout()
    {
        assertEquals( TimeUnit.SECONDS.toMillis( 60 ), Config.defaultConfig().connectionAcquisitionTimeoutMillis() );
    }

    @Test
    void shouldAllowPositiveConnectionAcquisitionTimeout()
    {
        Config config = Config.builder().withConnectionAcquisitionTimeout( 42, TimeUnit.SECONDS ).build();

        assertEquals( TimeUnit.SECONDS.toMillis( 42 ), config.connectionAcquisitionTimeoutMillis() );
    }

    @Test
    void shouldAllowNegativeConnectionAcquisitionTimeout()
    {
        Config config = Config.builder().withConnectionAcquisitionTimeout( -42, TimeUnit.HOURS ).build();

        assertEquals( -1, config.connectionAcquisitionTimeoutMillis() );
    }

    @Test
    void shouldAllowConnectionAcquisitionTimeoutOfZero()
    {
        Config config = Config.builder().withConnectionAcquisitionTimeout( 0, TimeUnit.DAYS ).build();

        assertEquals( 0, config.connectionAcquisitionTimeoutMillis() );
    }

    @Test
    void shouldEnableAndDisableHostnameVerificationOnTrustStrategy()
    {
        Config.TrustStrategy trustStrategy = Config.TrustStrategy.trustSystemCertificates();
        assertTrue( trustStrategy.isHostnameVerificationEnabled() );

        assertSame( trustStrategy, trustStrategy.withHostnameVerification() );
        assertTrue( trustStrategy.isHostnameVerificationEnabled() );

        assertSame( trustStrategy, trustStrategy.withoutHostnameVerification() );
        assertFalse( trustStrategy.isHostnameVerificationEnabled() );
    }

    @Test
    void shouldAllowToConfigureResolver()
    {
        ServerAddressResolver resolver = mock( ServerAddressResolver.class );
        Config config = Config.builder().withResolver( resolver ).build();

        assertEquals( resolver, config.resolver() );
    }

    @Test
    void shouldNotAllowNullResolver()
    {
        assertThrows( NullPointerException.class, () -> Config.builder().withResolver( null ) );
    }
}
