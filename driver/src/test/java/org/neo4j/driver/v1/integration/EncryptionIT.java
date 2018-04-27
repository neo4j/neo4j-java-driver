/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.v1.integration;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.util.Neo4jSettings;
import org.neo4j.driver.v1.util.Neo4jSettings.BoltTlsLevel;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.util.ServerVersion.v3_1_0;

public class EncryptionIT
{
    @Rule
    public final TestNeo4j neo4j = new TestNeo4j();

    private ServerVersion neo4jVersion;

    @Before
    public void setUp()
    {
        neo4jVersion = neo4j.version();
    }

    @Test
    public void shouldOperateWithNoEncryptionWhenItIsOptionalInTheDatabase()
    {
        testMatchingEncryption( BoltTlsLevel.OPTIONAL, false );
    }

    @Test
    public void shouldOperateWithEncryptionWhenItIsOptionalInTheDatabase()
    {
        testMatchingEncryption( BoltTlsLevel.OPTIONAL, true );
    }

    @Test
    public void shouldFailWithoutEncryptionWhenItIsRequiredInTheDatabase()
    {
        testMismatchingEncryption( BoltTlsLevel.REQUIRED, false );
    }

    @Test
    public void shouldOperateWithEncryptionWhenItIsAlsoRequiredInTheDatabase()
    {
        testMatchingEncryption( BoltTlsLevel.REQUIRED, true );
    }

    @Test
    public void shouldFailWithEncryptionWhenItIsDisabledInTheDatabase()
    {
        testMismatchingEncryption( BoltTlsLevel.DISABLED, true );
    }

    @Test
    public void shouldOperateWithoutEncryptionWhenItIsAlsoDisabledInTheDatabase()
    {
        testMatchingEncryption( BoltTlsLevel.DISABLED, false );
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

        try
        {
            GraphDatabase.driver( neo4j.uri(), neo4j.authToken(), config ).close();
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
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
    }

    private static Config newConfig( boolean withEncryption )
    {
        return withEncryption ? configWithEncryption() : configWithoutEncryption();
    }

    private static Config configWithEncryption()
    {
        return Config.build().withEncryption().toConfig();
    }

    private static Config configWithoutEncryption()
    {
        return Config.build().withoutEncryption().toConfig();
    }
}
