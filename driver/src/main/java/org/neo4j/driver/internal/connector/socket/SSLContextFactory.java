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
package org.neo4j.driver.internal.connector.socket;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.exceptions.ClientException;

import static org.neo4j.driver.internal.util.CertificateTool.loadX509Cert;

class SSLContextFactory
{
    private final String host;
    private final int port;
    private final Config.TrustStrategy authConfig;
    private final Logger logger;

    SSLContextFactory( String host, int port, Config.TrustStrategy authConfig, Logger logger )
    {
        this.host = host;
        this.port = port;
        this.authConfig = authConfig;
        this.logger = logger;
    }

    public SSLContext create()
            throws GeneralSecurityException, IOException
    {
        SSLContext sslContext = SSLContext.getInstance( "TLS" );
        TrustManager[] trustManagers;

        switch ( authConfig.strategy() ) {
        case TRUST_SIGNED_CERTIFICATES:
            logger.warn( "Option `TRUST_SIGNED_CERTIFICATE` has been deprecated and will be removed in a future version " +
                         "of the driver. Please switch to use `TRUST_CUSTOM_CA_SIGNED_CERTIFICATES` instead." );
            //intentional fallthrough
        case TRUST_CUSTOM_CA_SIGNED_CERTIFICATES:
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

            break;

        //just rely on system defaults
        case TRUST_SYSTEM_CA_SIGNED_CERTIFICATES:
            return SSLContext.getDefault();

        case TRUST_ON_FIRST_USE:
            trustManagers = new TrustManager[]{new TrustOnFirstUseTrustManager( host, port, authConfig.certFile(), logger )};
            break;
        default:
            throw new ClientException( "Unknown TLS authentication strategy: " + authConfig.strategy().name() );
        }

        sslContext.init( new KeyManager[0], trustManagers, null );
        return sslContext;
    }
}
