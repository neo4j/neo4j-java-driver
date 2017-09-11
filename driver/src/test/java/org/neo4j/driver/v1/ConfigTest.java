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
package org.neo4j.driver.v1;

import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.util.FileTools;

import static java.lang.System.getProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConfigTest
{
    private static final File DEFAULT_KNOWN_HOSTS = new File( getProperty( "user.home" ),
            ".neo4j" + File.separator + "known_hosts" );

    @Test
    public void shouldDefaultToKnownCerts()
    {
        // Given
        Config config = Config.defaultConfig();

        // When
        Config.TrustStrategy authConfig = config.trustStrategy();

        // Then
        assertEquals( authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES );
    }

    @SuppressWarnings( "deprecation" )
    @Test
    public void shouldChangeToNewKnownCerts()
    {
        // Given
        File knownCerts = new File( "new_known_hosts" );
        Config config = Config.build().withTrustStrategy( Config.TrustStrategy.trustOnFirstUse( knownCerts ) ).toConfig();

        // When
        Config.TrustStrategy authConfig = config.trustStrategy();

        // Then
        assertEquals( authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_ON_FIRST_USE );
        assertEquals( knownCerts.getAbsolutePath(), authConfig.certFile().getAbsolutePath() );
    }

    @Test
    public void shouldChangeToTrustedCert()
    {
        // Given
        File trustedCert = new File( "trusted_cert" );
        Config config = Config.build().withTrustStrategy( Config.TrustStrategy.trustCustomCertificateSignedBy( trustedCert ) ).toConfig();

        // When
        Config.TrustStrategy authConfig = config.trustStrategy();

        // Then
        assertEquals( authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES );
        assertEquals( trustedCert.getAbsolutePath(), authConfig.certFile().getAbsolutePath() );
    }

    @Test
    public void shouldSupportLivenessCheckTimeoutSetting() throws Throwable
    {
        Config config = Config.build().withConnectionLivenessCheckTimeout( 42, TimeUnit.SECONDS ).toConfig();

        assertEquals( TimeUnit.SECONDS.toMillis( 42 ), config.idleTimeBeforeConnectionTest() );
    }

    @Test
    public void shouldAllowZeroConnectionLivenessCheckTimeout() throws Throwable
    {
        Config config = Config.build().withConnectionLivenessCheckTimeout( 0, TimeUnit.SECONDS ).toConfig();

        assertEquals( 0, config.idleTimeBeforeConnectionTest() );
    }

    @Test
    public void shouldAllowNegativeConnectionLivenessCheckTimeout() throws Throwable
    {
        Config config = Config.build().withConnectionLivenessCheckTimeout( -42, TimeUnit.SECONDS ).toConfig();

        assertEquals( TimeUnit.SECONDS.toMillis( -42 ), config.idleTimeBeforeConnectionTest() );
    }

    @Test
    public void shouldSupportMaxConnectionLifetimeSetting() throws Throwable
    {
        Config config = Config.build().withMaxConnectionLifetime( 42, TimeUnit.SECONDS ).toConfig();

        assertEquals( TimeUnit.SECONDS.toMillis( 42 ), config.maxConnectionLifetimeMillis() );
    }

    @Test
    public void shouldAllowZeroConnectionMaxConnectionLifetime() throws Throwable
    {
        Config config = Config.build().withMaxConnectionLifetime( 0, TimeUnit.SECONDS ).toConfig();

        assertEquals( 0, config.maxConnectionLifetimeMillis() );
    }

    @Test
    public void shouldAllowNegativeConnectionMaxConnectionLifetime() throws Throwable
    {
        Config config = Config.build().withMaxConnectionLifetime( -42, TimeUnit.SECONDS ).toConfig();

        assertEquals( TimeUnit.SECONDS.toMillis( -42 ), config.maxConnectionLifetimeMillis() );
    }

    @Test
    public void shouldTurnOnLeakedSessionsLogging()
    {
        // leaked sessions logging is turned off by default
        assertFalse( Config.build().toConfig().logLeakedSessions() );

        // it can be turned on using config
        assertTrue( Config.build().withLeakedSessionsLogging().toConfig().logLeakedSessions() );
    }

    @Test
    public void shouldHaveDefaultConnectionTimeout()
    {
        Config defaultConfig = Config.defaultConfig();
        assertEquals( TimeUnit.SECONDS.toMillis( 5 ), defaultConfig.connectionTimeoutMillis() );
    }

    @Test
    public void shouldRespectConfiguredConnectionTimeout()
    {
        Config config = Config.build().withConnectionTimeout( 42, TimeUnit.HOURS ).toConfig();
        assertEquals( TimeUnit.HOURS.toMillis( 42 ), config.connectionTimeoutMillis() );
    }

    @Test
    public void shouldAllowConnectionTimeoutOfZero()
    {
        Config config = Config.build().withConnectionTimeout( 0, TimeUnit.SECONDS ).toConfig();
        assertEquals( 0, config.connectionTimeoutMillis() );
    }

    @Test
    public void shouldThrowForNegativeConnectionTimeout()
    {
        Config.ConfigBuilder builder = Config.build();

        try
        {
            builder.withConnectionTimeout( -42, TimeUnit.SECONDS );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void shouldThrowForTooLargeConnectionTimeout()
    {
        Config.ConfigBuilder builder = Config.build();

        try
        {
            builder.withConnectionTimeout( Long.MAX_VALUE - 42, TimeUnit.SECONDS );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void shouldNotAllowNegativeMaxRetryTimeMs()
    {
        Config.ConfigBuilder builder = Config.build();

        try
        {
            builder.withMaxTransactionRetryTime( -42, TimeUnit.SECONDS );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void shouldAllowZeroMaxRetryTimeMs()
    {
        Config config = Config.build().withMaxTransactionRetryTime( 0, TimeUnit.SECONDS ).toConfig();

        assertEquals( 0, config.retrySettings().maxRetryTimeMs() );
    }

    @Test
    public void shouldAllowPositiveRetryAttempts()
    {
        Config config = Config.build().withMaxTransactionRetryTime( 42, TimeUnit.SECONDS ).toConfig();

        assertEquals( TimeUnit.SECONDS.toMillis( 42 ), config.retrySettings().maxRetryTimeMs() );
    }

    public static void deleteDefaultKnownCertFileIfExists()
    {
        if( DEFAULT_KNOWN_HOSTS.exists() )
        {
            FileTools.deleteFile( DEFAULT_KNOWN_HOSTS );
        }
    }

}
