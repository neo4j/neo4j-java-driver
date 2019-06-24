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
package org.neo4j.driver.integration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.Neo4jSettings;
import org.neo4j.driver.util.Neo4jSettings.BoltTlsLevel;
import org.neo4j.driver.util.ParallelizableIT;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.util.ServerVersion.v3_1_0;

@ParallelizableIT
class EncryptionIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private ServerVersion neo4jVersion;

    @BeforeEach
    void setUp()
    {
        neo4jVersion = neo4j.version();
    }

    @Test
    void shouldConnectWhenItIsOptionalInTheDatabase()
    {
        testMatchingEncryption( BoltTlsLevel.OPTIONAL, false );
        testMatchingEncryption( BoltTlsLevel.OPTIONAL, true );
    }

//    @Test TODO requires boltkit to response when encryption is forced on
    void shouldOperateWhenEncryptionIsRequiredInTheDatabase()
    {
        testMatchingEncryption( BoltTlsLevel.REQUIRED, true );
        testMismatchingEncryption( BoltTlsLevel.REQUIRED, false );
    }

    @Test
    void shouldOperateWhenEncryptionIsDisabledInTheDatabase()
    {
        testMatchingEncryption( BoltTlsLevel.DISABLED, false );
        testMismatchingEncryption( BoltTlsLevel.DISABLED, true );
    }

    private void testMatchingEncryption( BoltTlsLevel tlsLevel, boolean driverEncrypted )
    {
        neo4j.restartDb( Neo4jSettings.TEST_SETTINGS.updateWith( Neo4jSettings.BOLT_TLS_LEVEL, tlsLevel.toString() ) );
        Config config = newConfig( driverEncrypted );

        try ( Driver driver = GraphDatabase.driver( neo4j.uri(), neo4j.authToken(), config ) )
        {
            assertThat( driver.isEncrypted(), equalTo( driverEncrypted ) );

            try ( Session session = driver.session() )
            {
                StatementResult result = session.run( "RETURN 1" );

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

        RuntimeException e = assertThrows( RuntimeException.class,
                () -> GraphDatabase.driver( neo4j.uri(), neo4j.authToken(), config ).close() );

        // pre 3.1 neo4j throws different exception when encryption required but not used
        if ( neo4jVersion.lessThan( v3_1_0 ) && tlsLevel == BoltTlsLevel.REQUIRED )
        {
            assertThat( e, instanceOf( ClientException.class ) );
            assertThat( e.getMessage(), startsWith( "This server requires a TLS encrypted connection" ) );
        }
        else
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
            assertThat( e.getMessage(), startsWith( "Connection to the database terminated" ) );
        }
    }

    private static Config newConfig( boolean withEncryption )
    {
        return withEncryption ? configWithEncryption() : configWithoutEncryption();
    }

    private static Config configWithEncryption()
    {
        return Config.builder().withEncryption().build();
    }

    private static Config configWithoutEncryption()
    {
        return Config.builder().withoutEncryption().build();
    }
}
