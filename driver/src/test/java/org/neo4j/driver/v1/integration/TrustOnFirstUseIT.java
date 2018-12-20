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
package org.neo4j.driver.v1.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;

import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.SecurityException;
import org.neo4j.driver.v1.util.CertificateToolUtil;
import org.neo4j.driver.v1.util.DatabaseExtension;
import org.neo4j.driver.v1.util.ParallelizableIT;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.v1.Config.TrustStrategy.trustOnFirstUse;
import static org.neo4j.driver.v1.util.FileTools.tempFile;
import static org.neo4j.driver.v1.util.TestUtil.getRootCause;

@ParallelizableIT
class TrustOnFirstUseIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void shouldTrustOnFirstUse() throws Throwable
    {
        // Given
        File knownHostsFile = tempFile( "known_hosts" );
        // When & Then
        shouldBeAbleToRunCypher( () -> createDriverWithKnownHostsFile( knownHostsFile ) );
    }

    @Test
    void shouldTrustInSubsequentUse() throws Throwable
    {
        // Given
        File knownHostsFile = givenAnExistingKnownHostsFile();

        // When & Then
        shouldBeAbleToRunCypher( () -> createDriverWithKnownHostsFile( knownHostsFile ) );
    }

    @Test
    void shouldProtectFromManInTheMiddleAttack() throws Throwable
    {
        // Given
        File knownHostsFile = givenAnExistingKnownHostsFile();
        // When
        changeServerCertificate();
        // Then
        shouldFailToRunCypherWithAMeaningfulError( () -> createDriverWithKnownHostsFile( knownHostsFile ) );
    }

    @Test
    void shouldNotInterfereWithEachOther() throws Throwable
    {
        // Given
        File knownHostsFile = givenAnExistingKnownHostsFile();
        File anotherKnownHostsFile = tempFile( "known_hosts" );

        // When
        changeServerCertificate();
        // Then
        shouldFailToRunCypherWithAMeaningfulError( () -> createDriverWithKnownHostsFile( knownHostsFile ) );
        shouldBeAbleToRunCypher( () -> createDriverWithKnownHostsFile( anotherKnownHostsFile ) );
    }

    private void changeServerCertificate() throws Throwable
    {
        CertificateToolUtil.CertificateKeyPair<File,File> server = CertificateToolUtil.createNewCertificateAndKey();
        neo4j.updateEncryptionKeyAndCert( server.key(), server.cert() );
    }

    private File givenAnExistingKnownHostsFile() throws Throwable
    {
        File knownHostsFile = tempFile( "known_hosts" );
        shouldBeAbleToRunCypher( () -> createDriverWithKnownHostsFile( knownHostsFile ) );
        return knownHostsFile;
    }

    private Driver createDriverWithKnownHostsFile( File knownHostsFile )
    {
        return GraphDatabase.driver( neo4j.uri(), neo4j.authToken(),
                Config.builder().withEncryption().withTrustStrategy( trustOnFirstUse( knownHostsFile ) ).build() );
    }

    private void shouldFailToRunCypherWithAMeaningfulError( Supplier<Driver> driverSupplier )
    {
        SecurityException exception = assertThrows( SecurityException.class, driverSupplier::get );
        Throwable rootCause = getRootCause( exception );
        assertThat( rootCause.toString(), containsString(
                "Unable to connect to neo4j at `localhost:" + neo4j.boltPort() + "`, " +
                        "because the certificate the server uses has changed. " +
                        "This is a security feature to protect against man-in-the-middle attacks." ) );
        assertThat( rootCause.toString(), containsString(
                "If you trust the certificate the server uses now, simply remove the line that starts with " +
                        "`localhost:" + neo4j.boltPort() + "` in the file" ) );
        assertThat( rootCause.toString(), containsString( "The old certificate saved in file is:" ) );
        assertThat( rootCause.toString(), containsString( "The New certificate received is:" ) );
    }

    private void shouldBeAbleToRunCypher( Supplier<Driver> driverSupplier )
    {
        try ( Driver driver = driverSupplier.get(); Session session = driver.session() )
        {
            StatementResult result = session.run( "RETURN 1 as n" );
            assertThat( result.single().get( "n" ).asInt(), equalTo( 1 ) );
        }
    }
}
