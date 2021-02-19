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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.Scheme;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.Neo4jSettings;
import org.neo4j.driver.util.Neo4jSettings.BoltTlsLevel;
import org.neo4j.driver.util.ParallelizableIT;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Config.TrustStrategy.trustAllCertificates;

@ParallelizableIT
class EncryptionIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void shouldOperateWithNoEncryptionWhenItIsOptionalInTheDatabase()
    {
        testMatchingEncryption( BoltTlsLevel.OPTIONAL, false, neo4j.uri().getScheme() );
    }

    @Test
    void shouldOperateWithEncryptionWhenItIsOptionalInTheDatabase()
    {
        testMatchingEncryption( BoltTlsLevel.OPTIONAL, true, neo4j.uri().getScheme() );
    }

    @Test
    void shouldFailWithoutEncryptionWhenItIsRequiredInTheDatabase()
    {
        testMismatchingEncryption( BoltTlsLevel.REQUIRED, false );
    }

    @Test
    void shouldOperateWithEncryptionWhenItIsAlsoRequiredInTheDatabase()
    {
        testMatchingEncryption( BoltTlsLevel.REQUIRED, true, neo4j.uri().getScheme() );
    }

    @Test
    void shouldOperateWithEncryptionWhenConfiguredUsingBoltSscURI()
    {
        testMatchingEncryption( BoltTlsLevel.REQUIRED, true, "bolt+ssc" );
    }

    @Test
    void shouldFailWithEncryptionWhenItIsDisabledInTheDatabase()
    {
        testMismatchingEncryption( BoltTlsLevel.DISABLED, true );
    }

    @Test
    void shouldOperateWithoutEncryptionWhenItIsAlsoDisabledInTheDatabase()
    {
        testMatchingEncryption( BoltTlsLevel.DISABLED, false, neo4j.uri().getScheme() );
    }

    private void testMatchingEncryption( BoltTlsLevel tlsLevel, boolean driverEncrypted, String scheme )
    {
        neo4j.restartDb( Neo4jSettings.TEST_SETTINGS.updateWith( Neo4jSettings.BOLT_TLS_LEVEL, tlsLevel.toString() ) );
        Config config;

        if ( scheme.equals( Scheme.BOLT_LOW_TRUST_URI_SCHEME ) )
        {
            config = Config.builder().build();
        } else {
            config = newConfig( driverEncrypted );
        }

        URI uri = URI.create( String.format( "%s://%s:%s", scheme, neo4j.uri().getHost(), neo4j.uri().getPort()) );

        try ( Driver driver = GraphDatabase.driver( uri, neo4j.authToken(), config ) )
        {
            assertThat( driver.isEncrypted(), equalTo( driverEncrypted ) );

            try ( Session session = driver.session() )
            {
                Result result = session.run( "RETURN 1" );

                Record record = result.next();
                int value = record.get( 0 ).asInt();
                assertThat( value, equalTo( 1 ) );
            }
        }
    }

    private void testMismatchingEncryption( BoltTlsLevel tlsLevel, boolean driverEncrypted )
    {
        neo4j.restartDb( Neo4jSettings.TEST_SETTINGS.updateWith( Neo4jSettings.BOLT_TLS_LEVEL, tlsLevel.toString() ) );
        Config config = newConfig( driverEncrypted );

        ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class,
                () -> GraphDatabase.driver( neo4j.uri(), neo4j.authToken(), config ).verifyConnectivity() );

        assertThat( e.getMessage(), startsWith( "Connection to the database terminated" ) );
    }

    private static Config newConfig( boolean withEncryption )
    {
        return withEncryption ? configWithEncryption() : configWithoutEncryption();
    }

    private static Config configWithEncryption()
    {
        return Config.builder().withEncryption().withTrustStrategy( trustAllCertificates() ).build();
    }

    private static Config configWithoutEncryption()
    {
        return Config.builder().withoutEncryption().build();
    }
}
