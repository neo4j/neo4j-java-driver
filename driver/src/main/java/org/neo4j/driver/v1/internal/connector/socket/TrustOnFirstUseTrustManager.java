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

import static org.neo4j.driver.v1.internal.util.CertificateTool.X509CertToString;

/**
 * References:
 * http://stackoverflow.com/questions/6802421/how-to-compare-distinct-implementations-of-java-security-cert-x509certificate?answertab=votes#tab-top
 */

class TrustOnFirstUseTrustManager implements X509TrustManager
{
    /**
     * A list of pairs (known_server, certificate) are stored in this file.
     * When establishing a SSL connection to a new server, we will save the server's ip:port and its certificate in this
     * file.
     * Then when we try to connect to a known server again, we will authenticate the server by checking if it provides
     * the same certificate as the one saved in this file.
     */
    private final File knownCerts;

    /** The server ip:port (in digits) of the server that we are currently connected to */
    private final String serverId;

    /** The known certificate we've registered for this server */
    private String cert;

    TrustOnFirstUseTrustManager( String host, int port, File knownCerts ) throws IOException
    {
        String ip = InetAddress.getByName( host ).getHostAddress(); // localhost -> 127.0.0.1
        this.serverId = ip + ":" + port;

        this.knownCerts = knownCerts;
        load();
    }

    /**
     * Try to load the certificate form the file if the server we've connected is a known server.
     *
     * @throws IOException
     */
    private void load() throws IOException
    {
        if ( !knownCerts.exists() )
        {
            return;
        }

        BufferedReader reader = new BufferedReader( new FileReader( knownCerts ) );
        String line = null;
        while ( (line = reader.readLine()) != null )
        {
            if ( (!line.trim().startsWith( "#" )) && line.contains( "," ) )
            {
                String[] strings = line.split( "," );
                if ( strings[0].trim().equals( serverId ) )
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

        createKnownCertFileIfNotExists();

        BufferedWriter writer = new BufferedWriter( new FileWriter( knownCerts, true ) );
        writer.write( serverId + "," + this.cert );
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
                        "Failed to save the server ID and the certificate received from the server to file %s.\n" +
                        "Server ID: %s\nReceived cert:\n%s",
                        knownCerts.getAbsolutePath(), serverId, X509CertToString( cert ) ), e );
            }
        }
        else
        {
            if ( !this.cert.equals( cert ) )
            {
                throw new CertificateException( String.format(
                        "Unable to connect to neo4j at `%s`, because the certificate the server uses has changed. " +
                        "This is a security feature to protect against man-in-the-middle attacks.\n" +
                        "If you trust the certificate the server uses now, simply remove the line that starts with " +
                        "`%s` " +
                        "in the file `%s`.\n" +
                        "The old certificate saved in file is:\n%sThe New certificate received is:\n%s",
                        serverId, serverId, knownCerts.getAbsolutePath(),
                        X509CertToString( this.cert ), X509CertToString( cert ) ) );
            }
        }
    }

    private File createKnownCertFileIfNotExists() throws IOException
    {
        if ( !knownCerts.exists() )
        {
            File parentDir = knownCerts.getParentFile();
            if( parentDir != null && !parentDir.exists() )
            {
                parentDir.mkdirs();
            }
            knownCerts.createNewFile();
            BufferedWriter writer = new BufferedWriter( new FileWriter( knownCerts ) );
            writer.write( "# This file contains trusted certificates for Neo4j servers, it's created by Neo4j drivers." );
            writer.newLine();
            writer.write( "# You can configure the location of this file in `org.neo4j.driver.Config`" );
            writer.newLine();
            writer.close();
        }

        return knownCerts;
    }

    /**
     * No issuer is trusted.
     */
    public X509Certificate[] getAcceptedIssuers()
    {
        return new X509Certificate[0];
    }
}
