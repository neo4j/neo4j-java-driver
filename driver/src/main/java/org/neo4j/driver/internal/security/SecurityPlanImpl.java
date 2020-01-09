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
package org.neo4j.driver.internal.security;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import static org.neo4j.driver.internal.util.CertificateTool.loadX509Cert;

/**
 * A SecurityPlan consists of encryption and trust details.
 */
public class SecurityPlanImpl implements SecurityPlan
{
    public static SecurityPlan forAllCertificates( boolean requiresHostnameVerification ) throws GeneralSecurityException
    {
        SSLContext sslContext = SSLContext.getInstance( "TLS" );
        sslContext.init( new KeyManager[0], new TrustManager[]{new TrustAllTrustManager()}, null );

        return new SecurityPlanImpl( true, sslContext, requiresHostnameVerification );
    }

    public static SecurityPlan forCustomCASignedCertificates( File certFile, boolean requiresHostnameVerification )
            throws GeneralSecurityException, IOException
    {
        // A certificate file is specified so we will load the certificates in the file
        // Init a in memory TrustedKeyStore
        KeyStore trustedKeyStore = KeyStore.getInstance( "JKS" );
        trustedKeyStore.load( null, null );

        // Load the certs from the file
        loadX509Cert( certFile, trustedKeyStore );

        // Create TrustManager from TrustedKeyStore
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance( "SunX509" );
        trustManagerFactory.init( trustedKeyStore );

        SSLContext sslContext = SSLContext.getInstance( "TLS" );
        sslContext.init( new KeyManager[0], trustManagerFactory.getTrustManagers(), null );

        return new SecurityPlanImpl( true, sslContext, requiresHostnameVerification );
    }

    public static SecurityPlan forSystemCASignedCertificates( boolean requiresHostnameVerification ) throws NoSuchAlgorithmException
    {
        return new SecurityPlanImpl( true, SSLContext.getDefault(), requiresHostnameVerification );
    }

    public static SecurityPlan insecure()
    {
        return new SecurityPlanImpl( false, null, false );
    }

    private final boolean requiresEncryption;
    private final SSLContext sslContext;
    private final boolean requiresHostnameVerification;

    private SecurityPlanImpl( boolean requiresEncryption, SSLContext sslContext, boolean requiresHostnameVerification )
    {
        this.requiresEncryption = requiresEncryption;
        this.sslContext = sslContext;
        this.requiresHostnameVerification = requiresHostnameVerification;
    }

    @Override
    public boolean requiresEncryption()
    {
        return requiresEncryption;
    }

    @Override
    public SSLContext sslContext()
    {
        return sslContext;
    }

    @Override
    public boolean requiresHostnameVerification()
    {
        return requiresHostnameVerification;
    }

    private static class TrustAllTrustManager implements X509TrustManager
    {
        public void checkClientTrusted( X509Certificate[] chain, String authType ) throws CertificateException
        {
            throw new CertificateException( "All client connections to this client are forbidden." );
        }

        public void checkServerTrusted( X509Certificate[] chain, String authType ) throws CertificateException
        {
            // all fine, pass through
        }

        public X509Certificate[] getAcceptedIssuers()
        {
            return new X509Certificate[0];
        }
    }
}
