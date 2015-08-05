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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.connector.socket;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;
import javax.xml.bind.DatatypeConverter;

/**
 * References:
 * http://stackoverflow.com/questions/6802421/how-to-compare-distinct-implementations-of-java-security-cert-x509certificate?answertab=votes#tab-top
 */

class TrustOnFirstUseTrustManager implements X509TrustManager
{
    // TODO: discussion: do we really need the ip? Is it enough if we just create a trusted store from the first cert
    // that we've got and trust the single cert?
    /**
     * A list of pairs (known_server_ip, certificate) are stored in this file.
     * When establishing a SSL connection to a new server, we will save the server's IP and its certificate in this
     * file.
     * Then when we try to connect to a known server again, we will authenticate the server by checking if it provides
     * the same certificate as the one saved in this file.
     */
    static final String KNOWN_CERTS_FILE_PATH = "known_certs";

    /** The ip address of the server that we are currently connected to */
    private final String ip;

    /** The known certificate we've registered for this server */
    private String cert;

    TrustOnFirstUseTrustManager( String host ) throws IOException
    {
        // retrieve ip from host
        InetAddress address = InetAddress.getByName( host );
        this.ip = address.getHostAddress();

        load();
    }

    /**
     * Try to load the certificate form the file if the server we've connected is a known server.
     *
     * @throws IOException
     */
    private void load() throws IOException
    {
        File knownCertsFile = new File( KNOWN_CERTS_FILE_PATH );
        if ( !knownCertsFile.exists() ) // it is fine if this file dose not exist
        {
            knownCertsFile.createNewFile();
            return;
        }

        BufferedReader reader = new BufferedReader( new FileReader( knownCertsFile ) );
        String line = null;
        while ( (line = reader.readLine()) != null )
        {
            if ( line.contains( "," ) )
            {
                String[] strings = line.split( "," );
                if ( strings[0].trim().equals( ip ) )
                {
                    // load the certificate
                    cert = strings[1].trim();
                    return;
                }
            }
        }
        reader.close();
    }

    /**
     * Save a new (server_ip, cert) pair into knownCerts file
     *
     * @param cert
     */
    private void save( String cert ) throws IOException
    {
        this.cert = cert;

        File knownCertsFile = new File( KNOWN_CERTS_FILE_PATH );
        if ( !knownCertsFile.exists() ) // it is fine if this file dose not exist
        {
            knownCertsFile.createNewFile();
        }

        BufferedWriter writer = new BufferedWriter( new FileWriter( knownCertsFile, true ) );
        writer.write( ip + "," + this.cert );
        writer.newLine();
        writer.close();
    }

    /*
     * Disallow all client connection to this client
     */
    public void checkClientTrusted( X509Certificate[] chain, String authType )
            throws CertificateException
    {
        throw new CertificateException( "All client connections to this client are forbidden." );
    }

    /*
     * Trust the cert if it is seen first time for this server or it is the same with the one registered.
     */
    public void checkServerTrusted( X509Certificate[] chain, String authType )
            throws CertificateException
    {
        X509Certificate certificate = chain[0];
        byte[] encoded = certificate.getEncoded();

        String cert = DatatypeConverter.printBase64Binary( encoded );

        if ( this.cert == null )
        {
            try
            {
                save( cert );
            }
            catch ( IOException e )
            {
                throw new CertificateException( String.format(
                        "Failed to save the server's ip and the certificate received from the server to file %s.\n" +
                        "Server IP: %s\nReceived cert: %s",
                        new File( KNOWN_CERTS_FILE_PATH ).getAbsolutePath(), ip, cert ), e );
            }
        }
        else if ( !this.cert.equals( cert ) )
        {
            throw new CertificateException( String.format(
                    "The certificate received from the server is different from the one we've known in file %s.\n" +
                    "Expect cert: %s\nReceived cert: %s",
                    new File( KNOWN_CERTS_FILE_PATH ).getAbsolutePath(), this.cert, cert ) );
        }
    }

    /**
     * No issuer is trusted.
     */
    public X509Certificate[] getAcceptedIssuers()
    {
        return new X509Certificate[0];
    }
}
