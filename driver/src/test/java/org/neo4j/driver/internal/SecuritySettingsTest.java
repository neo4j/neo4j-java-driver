/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

package org.neo4j.driver.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.security.NoSuchAlgorithmException;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;

import org.neo4j.driver.Config;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.security.SecurityPlan;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecuritySettingsTest
{
    private static Stream<String> selfSignedSchemes()
    {
        return Stream.of( "bolt+ssc", "neo4j+ssc" );
    }

    private static Stream<String> systemCertSchemes()
    {
        return Stream.of( "neo4j+s", "bolt+s" );
    }

    private static Stream<String> allSchemes()
    {
        return Stream.concat( selfSignedSchemes(), systemCertSchemes() );
    }

    @ParameterizedTest
    @MethodSource( "allSchemes" )
    void testEncryptionSchemeEnablesEncryption( String scheme )
    {
        SecuritySettings securitySettings = new SecuritySettings.SecuritySettingsBuilder().build();

        SecurityPlan securityPlan = securitySettings.createSecurityPlan( scheme );

        assertTrue( securityPlan.requiresEncryption() );
    }

    @ParameterizedTest
    @MethodSource( "systemCertSchemes" )
    void testSystemCertCompatibleConfiguration( String scheme ) throws Exception
    {
        SecuritySettings securitySettings = new SecuritySettings.SecuritySettingsBuilder().build();

        SecurityPlan securityPlan = securitySettings.createSecurityPlan( scheme );

        SSLContext defaultContext = SSLContext.getDefault();

        assertTrue( securityPlan.requiresEncryption() );
        assertEquals( defaultContext, securityPlan.sslContext() );
    }

    @ParameterizedTest
    @MethodSource( "allSchemes" )
    void testThrowsOnUserCustomizedEncryption( String scheme )
    {
        SecuritySettings securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                .withoutEncryption()
                .build();

        ClientException ex =
                assertThrows( ClientException.class,
                              () -> securitySettings.createSecurityPlan( scheme ) );

        assertTrue( ex.getMessage().contains( String.format( "Scheme %s is not configurable with manual encryption and trust settings", scheme ) ));
    }

    @ParameterizedTest
    @MethodSource( "allSchemes" )
    void testThrowsOnUserCustomizedTrustConfiguration( String scheme )
    {
        SecuritySettings securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                .withTrustStrategy( Config.TrustStrategy.trustSystemCertificates() )
                .build();

        ClientException ex =
                assertThrows( ClientException.class,
                              () -> securitySettings.createSecurityPlan( scheme ) );

        assertTrue( ex.getMessage().contains( String.format( "Scheme %s is not configurable with manual encryption and trust settings", scheme ) ));
    }

    @ParameterizedTest
    @MethodSource( "allSchemes" )
    void testThrowsOnUserCustomizedTrustConfigurationAndEncryption( String scheme )
    {
        SecuritySettings securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                .withTrustStrategy( Config.TrustStrategy.trustSystemCertificates() )
                .withEncryption()
                .build();

        ClientException ex =
                assertThrows( ClientException.class,
                              () -> securitySettings.createSecurityPlan( scheme ) );

        assertTrue( ex.getMessage().contains( String.format( "Scheme %s is not configurable with manual encryption and trust settings", scheme ) ));
    }

    @Test
    void testNeo4jSchemeNoEncryption()
    {
        SecuritySettings securitySettings = new SecuritySettings.SecuritySettingsBuilder().build();

        SecurityPlan securityPlan = securitySettings.createSecurityPlan( "neo4j" );

        assertFalse( securityPlan.requiresEncryption() );
    }

    @Test
    void testBoltSchemeNoEncryption()
    {
        SecuritySettings securitySettings = new SecuritySettings.SecuritySettingsBuilder().build();

        SecurityPlan securityPlan = securitySettings.createSecurityPlan( "bolt" );

        assertFalse( securityPlan.requiresEncryption() );
    }

    @Test
    void testConfiguredEncryption()
    {
        SecuritySettings securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                .withEncryption().build();

        SecurityPlan securityPlan = securitySettings.createSecurityPlan( "neo4j" );

        assertTrue( securityPlan.requiresEncryption() );
    }

    @Test
    void testConfiguredAllCertificates() throws NoSuchAlgorithmException
    {
        SecuritySettings securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                .withEncryption()
                .withTrustStrategy( Config.TrustStrategy.trustAllCertificates() )
                .build();

        SecurityPlan securityPlan = securitySettings.createSecurityPlan( "neo4j" );

        assertTrue( securityPlan.requiresEncryption() );
    }

}
