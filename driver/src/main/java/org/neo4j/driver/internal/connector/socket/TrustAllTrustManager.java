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

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.X509TrustManager;

/**
 * References:
 * http://stackoverflow.com/questions/6802421/how-to-compare-distinct-implementations-of-java-security-cert-x509certificate?answertab=votes#tab-top
 */
public class TrustAllTrustManager implements X509TrustManager
{

    /*
     * Disallow all client connection to this client
     */
    public void checkClientTrusted( X509Certificate[] chain, String authType )
            throws CertificateException
    {
        throw new CertificateException( "All client connections to this client are forbidden." );
    }

    /*
     * Always trust the cert
     */
    public void checkServerTrusted( X509Certificate[] chain, String authType )
            throws CertificateException
    {
        // trust everything
    }

    /**
     * No issuer is trusted.
     */
    public X509Certificate[] getAcceptedIssuers()
    {
        return new X509Certificate[0];
    }

}
