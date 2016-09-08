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
package org.neo4j.driver.v1.tck;

import cucumber.api.java.After;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import java.io.File;
import java.security.cert.X509Certificate;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Config.EncryptionLevel;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.CertificateToolTest.CertificateSigningRequestGenerator;
import org.neo4j.driver.v1.util.CertificateToolTest.SelfSignedCertificateGenerator;
import org.neo4j.driver.v1.util.Neo4jRunner;
import org.neo4j.driver.v1.util.Neo4jSettings;

import static java.io.File.createTempFile;
import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.internal.util.CertificateTool.saveX509Cert;
import static org.neo4j.driver.v1.Config.TrustStrategy.trustCustomCertificateSignedBy;
import static org.neo4j.driver.v1.Config.TrustStrategy.trustOnFirstUse;
import static org.neo4j.driver.v1.tck.DriverComplianceIT.neo4j;
import static org.neo4j.driver.v1.util.CertificateToolTest.generateSelfSignedCertificate;

public class DriverSecurityComplianceSteps
{
    private Driver driver;
    private File knownHostsFile;
    private Throwable exception;
    private Driver driverKitten; // well, just a reference to another driver

    // first use
    @Given( "^a running Neo4j Database$" )
    public void aRunningDatabase() throws Throwable
    {
    }

    @When( "I connect via a TLS-enabled transport for the first time for the given hostname and port$" )
    public void firstUseConnect() throws Throwable
    {
        knownHostsFile = tempFile( "known_hosts", ".tmp" );
        driver = GraphDatabase.driver(
                Neo4jRunner.DEFAULT_URI,
                Config.build().withEncryptionLevel( EncryptionLevel.REQUIRED )
                        .withTrustStrategy( trustOnFirstUse( knownHostsFile ) ).toConfig() );

    }

    @Then( "sessions should simply work$" )
    public void sessionsShouldSimplyWork() throws Throwable
    {
        try ( Session session = driver.session() )
        {
            StatementResult statementResult = session.run( "RETURN 1" );
            assertEquals( statementResult.single().get( 0 ).asInt(), 1 );
        }
    }

    // subsequent use
    @Given( "^a running Neo4j Database that I have connected to with a TLS-enabled transport in the past$" )
    public void aRunningNeoJDatabaseThatIHaveConnectedTo() throws Throwable
    {
        firstUseConnect();
        sessionsShouldSimplyWork();
    }

    @When( "^I connect via a TLS-enabled transport again$" )
    public void iConnectViaATlsEnabledTransportAgain() throws Throwable
    {
        driver = GraphDatabase.driver(
                Neo4jRunner.DEFAULT_URI,
                Config.build().withEncryptionLevel( EncryptionLevel.REQUIRED )
                        .withTrustStrategy( trustOnFirstUse( knownHostsFile ) ).toConfig() );
    }

    // man in the middle attack
    @And( "^the database has changed which certificate it uses$" )
    public void theDatabaseHasChangedWhichCertificateItUses() throws Throwable
    {
        driver.close();

        // create new certificate
        File cert = tempFile( "temp_cert", ".cert" );
        File key = tempFile( "temp_key", ".key" );

        SelfSignedCertificateGenerator generator = new SelfSignedCertificateGenerator();
        generator.saveSelfSignedCertificate( cert );
        generator.savePrivateKey( key );

        neo4j.updateEncryptionKeyAndCert( key, cert );

    }

    @Then( "^creating sessions should fail$" )
    public void creatingSessionsShouldFail() throws Throwable
    {
        try ( Session session = driver.session() ) {}
        catch ( Exception e )
        {
            exception = e;
        }
    }

    @And( "^I should get a helpful error explaining that the certificate has changed$" )
    public void iShouldGetAHelpfulErrorExplainingThatCertificateChanged( String str ) throws Throwable
    {
        assertThat( exception, notNullValue() );
        assertThat( exception, instanceOf( ClientException.class ) );
        Throwable rootCause = getRootCause( exception );
        assertThat( rootCause.toString(), containsString(
                "Unable to connect to neo4j at `localhost:7687`, because the certificate the server uses has changed. " +
                "This is a security feature to protect against man-in-the-middle attacks.") );
        assertThat( rootCause.toString(), containsString(
                "If you trust the certificate the server uses now, simply remove the line that starts with " +
                "`localhost:7687` in the file" ) );
        assertThat( rootCause.toString(), containsString( "The old certificate saved in file is:" ));
        assertThat( rootCause.toString(), containsString( "The New certificate received is:" ));
    }

    // modified trusted certificate file location
    @Given( "^two drivers" )
    public void twoDrivers() {}

    @When("^I configure one of them to use a different location for its known hosts storage$")
    public void twoDriversWithDifferentKnownHostsFiles() throws Throwable
    {
        firstUseConnect();
        sessionsShouldSimplyWork();

        File tempFile = tempFile( "known_hosts", ".tmp" );
        driverKitten = GraphDatabase.driver(
                Neo4jRunner.DEFAULT_URI,
                Config.build().withEncryptionLevel( EncryptionLevel.REQUIRED )
                        .withTrustStrategy( trustOnFirstUse( tempFile ) ).toConfig() );
    }

    @Then("^the two drivers should not interfere with one another's known hosts files$")
    public void twoDriversShouldNotInterfereWithEachOther() throws Throwable
    {
        // if I change the cert of the server, as driver has already connected, so driver will fall to connect
        theDatabaseHasChangedWhichCertificateItUses();
        iConnectViaATlsEnabledTransportAgain();
        creatingSessionsShouldFail();
        iShouldGetAHelpfulErrorExplainingThatCertificateChanged( "nah" );

        // However as driverKitten has not connected to the server, so driverKitten should just simply connect!
        try ( Session session = driverKitten.session() )
        {
            StatementResult statementResult = session.run( "RETURN 1" );
            assertEquals( statementResult.single().get( 0 ).asInt(), 1 );
        }
    }

    // signed certificate
    @Given("^a driver configured to use a trusted certificate$")
    public void aDriverConfiguredToUseATrustedCertificate() throws Throwable {}

    @And( "^a running Neo4j Database using a certificate signed by the same trusted certificate$" )
    public void aRunningNeo4jDatabaseUsingACertificateSignedByTheSameTrustedCertificate() throws Throwable
    {
        // create new root certificate
        File rootCert = tempFile( "temp_root_cert", ".cert" );
        File rootKey = tempFile( "temp_root_key", ".key" );

        SelfSignedCertificateGenerator certGenerator = new SelfSignedCertificateGenerator();
        certGenerator.saveSelfSignedCertificate( rootCert );
        certGenerator.savePrivateKey( rootKey );

        // give root certificate to driver
        driver = GraphDatabase.driver(
                Neo4jRunner.DEFAULT_URI,
                Config.build().withEncryptionLevel( EncryptionLevel.REQUIRED )
                        .withTrustStrategy( trustCustomCertificateSignedBy( rootCert ) ).toConfig() );

        // generate certificate signing request and get a certificate signed by the root private key
        File cert = tempFile( "temp_cert", ".cert" );
        File key = tempFile( "temp_key", ".key" );
        CertificateSigningRequestGenerator csrGenerator = new CertificateSigningRequestGenerator();
        X509Certificate signedCert = certGenerator.sign(
                csrGenerator.certificateSigningRequest(), csrGenerator.publicKey() );
        csrGenerator.savePrivateKey( key );
        saveX509Cert( signedCert, cert );

        neo4j.updateEncryptionKeyAndCert( key, cert );
    }

    @When( "^I connect via a TLS-enabled transport$" )
    public void iConnectViaATlsEnabledTransport() {}

    // same certificate
    @And( "^a running Neo4j Database using that exact trusted certificate$" )
    public void aRunningNeo4jDatabaseUsingThatExactTrustedCertificate()
    {
        driver = GraphDatabase.driver(
                Neo4jRunner.DEFAULT_URI,
                Config.build().withEncryptionLevel( EncryptionLevel.REQUIRED )
                        .withTrustStrategy( trustCustomCertificateSignedBy( Neo4jSettings.DEFAULT_TLS_CERT_FILE ) ).toConfig() );
    }

    // invalid cert
    @And("^a running Neo4j Database using a certificate not signed by the trusted certificate$")
    public void aRunningNeo4jDatabaseUsingACertNotSignedByTheTrustedCertificates() throws Throwable
    {
        File cert = tempFile( "temp_cert", ".cert" );
        saveX509Cert( generateSelfSignedCertificate(), cert );

        // give root certificate to driver
        driver = GraphDatabase.driver(
                Neo4jRunner.DEFAULT_URI,
                Config.build().withEncryptionLevel( EncryptionLevel.REQUIRED )
                        .withTrustStrategy( trustCustomCertificateSignedBy( cert ) ).toConfig() );
    }

    @And( "^I should get a helpful error explaining that no trusted certificate found$" )
    public void iShouldGetAHelpfulErrorExplainingThatCertificatedNotSigned() throws Throwable
    {
        assertThat( exception, notNullValue() );
        assertThat( exception, instanceOf( ClientException.class ) );
        Throwable rootCause = getRootCause( exception );
        assertThat( rootCause.toString(), containsString( "Signature does not match.") );
    }

    @After("@tls")
    public void clearAfterEachScenario() throws Throwable
    {
        driver.close();

        driver = null;
        knownHostsFile = null;
        exception = null;

        if( driverKitten != null )
        {
            driverKitten.close();
            driverKitten = null;
        }
    }

    @After("@modifies_db_config")
    public void resetDbWithDefaultSettings() throws Throwable
    {
        neo4j.restart();
    }

    private File tempFile(String prefix, String suffix) throws Throwable
    {
        File file = createTempFile( prefix, suffix );
        file.deleteOnExit();
        return file;
    }

    private Throwable getRootCause(Throwable th) {
        Throwable cause = th;
        while(cause.getCause() != null )
        {
            cause = cause.getCause();
        }
        return cause;
    }
}
