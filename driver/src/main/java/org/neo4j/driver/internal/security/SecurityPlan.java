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

package org.neo4j.driver.internal.security;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import static org.neo4j.driver.internal.util.CertificateTool.loadX509Cert;

/**
 * A SecurityPlan consists of encryption and trust details.
 */
public class SecurityPlan
{
    public static SecurityPlan forSignedCertificates( File certFile )
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

        return new SecurityPlan( true, sslContext);
    }

    public static SecurityPlan forSystemCertificates() throws NoSuchAlgorithmException, KeyStoreException
    {
        return new SecurityPlan( true, SSLContext.getDefault() );
    }


    public static SecurityPlan forTrustOnFirstUse( File knownHosts )
            throws IOException, KeyManagementException, NoSuchAlgorithmException
    {
        ConcurrentHashMap<String,String> preLoadKnownHostsMap = TrustOnFirstUseTrustManager.createKnownHostsMap( knownHosts );
        return new TrustOnFirstUseSecurityPlan( knownHosts, preLoadKnownHostsMap );
    }

    public static class TrustOnFirstUseSecurityPlan extends SecurityPlan
    {
        private final ConcurrentHashMap<String, String> trustOnFirstUseMap;
        private final File knownHostsFile;

        private TrustOnFirstUseSecurityPlan( File knownHostsFile, ConcurrentHashMap<String, String> trustOnFirstUseMap )
        {
            super( true, null );
            this.trustOnFirstUseMap = trustOnFirstUseMap;
            this.knownHostsFile = knownHostsFile;
        }

        public ConcurrentHashMap<String, String> trustOnFirstUseMap()
        {
            return this.trustOnFirstUseMap;
        }

        public File knownHostFile()
        {
            return this.knownHostsFile;
        }
    }

    public static SecurityPlan insecure()
    {
        return new SecurityPlan( false, null );
    }

    private final boolean requiresEncryption;
    private final SSLContext sslContext;

    private SecurityPlan( boolean requiresEncryption, SSLContext sslContext )
    {
        this.requiresEncryption = requiresEncryption;
        this.sslContext = sslContext;
    }

    public boolean requiresEncryption()
    {
        return requiresEncryption;
    }


    public SSLContext sslContext() {return sslContext;}

}
