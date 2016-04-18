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
package org.neo4j.driver.internal;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.v1.Config;

import static java.lang.System.getProperty;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ConfigTest
{

    @Rule
    public ExpectedException exception = ExpectedException.none();

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
        assertEquals( authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_ON_FIRST_USE );
        assertEquals( DEFAULT_KNOWN_HOSTS.getAbsolutePath(), authConfig.certFile().getAbsolutePath() );
    }

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
        Config config = Config.build().withTrustStrategy( Config.TrustStrategy.trustSignedBy( trustedCert ) ).toConfig();

        // When
        Config.TrustStrategy authConfig = config.trustStrategy();

        // Then
        assertEquals( authConfig.strategy(), Config.TrustStrategy.Strategy.TRUST_SIGNED_CERTIFICATES );
        assertEquals( trustedCert.getAbsolutePath(), authConfig.certFile().getAbsolutePath() );
    }

    @Test
    public void shouldConfigureMinIdleTime() throws Throwable
    {
        // when
        Config config = Config.build().withSessionLivenessCheckTimeout( 1337 ).toConfig();

        // then
        assertThat( config.idleTimeBeforeConnectionTest(), equalTo( 1337L ) );
    }

    @Test
    public void shouldConfigureFromMap() throws Throwable
    {
        // given
        Map<String, Object> map = new HashMap<>(  );
        map.put("trustStrategy", "TRUST_ON_FIRST_USE");
        map.put("knownHosts", "/dev/null/important_files");
        map.put("maxSessions", 42);
        map.put("sessionLivenessCheckTimeout", 1337L);
        map.put("encryptionLevel", "NONE");

        // when
        Config config = Config.build().fromMap( map ).toConfig();

        // then
        assertThat( config.idleTimeBeforeConnectionTest(), equalTo( 1337L ) );
        assertThat( config.connectionPoolSize(), equalTo( 42 ) );
        assertThat( config.trustStrategy(), equalTo( Config.TrustStrategy.trustOnFirstUse(
                new File( "/dev/null/important_files") ) ) );
        assertThat( config.idleTimeBeforeConnectionTest(), equalTo( 1337L ) );
        assertThat( config.encryptionLevel(), equalTo( Config.EncryptionLevel.NONE ) );
    }

    @Test
    public void shouldFailNicelyWhenInvalidOptions() throws Throwable
    {
        // given
        Map<String, Object> map = new HashMap<>(  );
        map.put("trustStrategy", "TRUST_ON_SECOND_USE");

        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "TRUST_ON_SECOND_USE is not a valid option for 'trustStrategy'. Valid values are " +
                                 "[TRUST_ON_FIRST_USE, TRUST_SIGNED_CERTIFICATES]" );

        // when
        Config.build().fromMap( map ).toConfig();
    }

    public static void deleteDefaultKnownCertFileIfExists()
    {
        if( DEFAULT_KNOWN_HOSTS.exists() )
        {
            DEFAULT_KNOWN_HOSTS.delete();
        }
    }

}
