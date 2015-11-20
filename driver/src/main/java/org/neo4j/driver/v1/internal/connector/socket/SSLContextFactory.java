/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.internal.connector.socket;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.neo4j.driver.v1.Config;

import static org.neo4j.driver.v1.internal.util.CertificateTool.loadX509Cert;

class SSLContextFactory
{

    private final String host;
    private final int port;
    private final Config.TlsAuthenticationConfig authConfig;

    SSLContextFactory( String host, int port, Config.TlsAuthenticationConfig authConfig )
    {
        this.host = host;
        this.port = port;
        this.authConfig = authConfig;
    }

    public SSLContext create()
            throws GeneralSecurityException, IOException
    {
        SSLContext sslContext = SSLContext.getInstance( "TLS" );

        // TODO Do we also want the server to verify the client's cert, a.k.a mutual authentication?
        // Ref: http://logicoy.com/blogs/ssl-keystore-truststore-and-mutual-authentication/
        KeyManager[] keyManagers = new KeyManager[0];
        TrustManager[] trustManagers = null;

        if ( authConfig.isFullAuthEnabled() )
        {
            // A certificate file is specified so we will load the certificates in the file
            // Init a in memory TrustedKeyStore
            KeyStore trustedKeyStore = KeyStore.getInstance( "JKS" );
            trustedKeyStore.load( null, null );

            // Load the certs from the file
            loadX509Cert( authConfig.certFile(), trustedKeyStore );

            // Create TrustManager from TrustedKeyStore
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance( "SunX509" );
            trustManagerFactory.init( trustedKeyStore );
            trustManagers = trustManagerFactory.getTrustManagers();
        }
        else
        {
            trustManagers = new TrustManager[]{new TrustOnFirstUseTrustManager( host, port, authConfig.certFile() )};
        }

        sslContext.init( keyManagers, trustManagers, null );
        return sslContext;
    }
}
