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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.junit.Test;

public class TrustAllTrustManagerTest
{

    @Test
    public void shouldTrustAllCerts() throws Throwable
    {
        // Given
    	TrustAllTrustManager manager = new TrustAllTrustManager();

        X509Certificate firstCertificate = mock( X509Certificate.class );
        when( firstCertificate.getEncoded() ).thenReturn( "fake certificate number 1".getBytes() );

        X509Certificate secondCertificate = mock( X509Certificate.class );
        when( secondCertificate.getEncoded() ).thenReturn( "fake certificate number 2".getBytes() );

        // When & Then
        try
        {
            manager.checkServerTrusted( new X509Certificate[]{firstCertificate}, null );
            manager.checkServerTrusted( new X509Certificate[]{secondCertificate}, null );
        }
        catch ( CertificateException e )
        {
        	fail( "Should trust all fake certificates" );
        }
    }

}
