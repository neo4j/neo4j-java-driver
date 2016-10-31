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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.X509TrustManager;

import org.neo4j.driver.internal.util.BytePrinter;
import org.neo4j.driver.v1.Logger;

import static java.lang.String.format;
import static org.neo4j.driver.internal.util.CertificateTool.X509CertToString;

/**
 * References:
 * http://stackoverflow.com/questions/6802421/how-to-compare-distinct-implementations-of-java-security-cert-x509certificate?answertab=votes#tab-top
 */
public class TrustOnFirstUseTrustManager implements X509TrustManager
{
    /**
     * A list of pairs (known_server certificate) are stored in this file.
     * When establishing a SSL connection to a new server, we will save the server's host:port and its certificate in this
     * file.
     * Then when we try to connect to a known server again, we will authenticate the server by checking if it provides
     * the same certificate as the one saved in this file.
     */
    private final File knownHostsFile;

    /** The map of server ip:port (in digits) and its known certificate we've registered */
    private final ConcurrentHashMap<String, String> knownHosts;
    private final Logger logger;
    private final String serverId;

    TrustOnFirstUseTrustManager( String serverId, File knownHosts, ConcurrentHashMap<String,String> preLoadedKnownHosts, Logger logger )
            throws IOException
    {
        this.logger = logger;
        this.knownHostsFile = knownHosts;
        this.knownHosts = preLoadedKnownHosts;
        this.serverId = serverId;
    }

    public static ConcurrentHashMap<String, String> createKnownHostsMap( File knownHostsFile ) throws IOException
    {
        ConcurrentHashMap<String, String> knownHosts = new ConcurrentHashMap<>();
        load( knownHostsFile, knownHosts );
        return knownHosts;
    }

    /**
     * Try to load the certificate form the file if the server we've connected is a known server.
     *
     * @throws IOException
     */
    private static synchronized void load( File knownHostsFile, Map<String, String> knownHosts ) throws IOException
    {
        if ( !knownHostsFile.exists() )
        {
            return;
        }

        assertKnownHostFileReadable( knownHostsFile );

        BufferedReader reader = new BufferedReader( new FileReader( knownHostsFile ) );
        String line;
        while ( (line = reader.readLine()) != null )
        {
            if ( (!line.trim().startsWith( "#" )) )
            {
                String[] strings = line.split( " " );
                if(strings.length == 2)
                {
                    // we need to load all serverId and finger prints from the file as we do not know which one is
                    // our current connection.
                    knownHosts.put( strings[0], strings[1] );
                }
            }
        }
        reader.close();
    }

    /**
     * Save a new (server_ip, cert) pair into knownHostsFile file
     *
     * @param fingerprint the SHA-512 fingerprint of the host certificate
     */
    private synchronized void saveTrustedHost( String serverId, String fingerprint ) throws IOException
    {

        knownHosts.put( serverId, fingerprint );
        logger.warn( "Adding %s as known and trusted certificate for %s.", fingerprint, serverId );
        createKnownCertFileIfNotExists();

        assertKnownHostFileWritable();
        BufferedWriter writer = new BufferedWriter( new FileWriter( knownHostsFile, true ) );
        writer.write( serverId + " " + fingerprint );
        writer.newLine();
        writer.close();
    }


    private static void assertKnownHostFileReadable( File knownHostsFile ) throws IOException
    {
        if( !knownHostsFile.canRead() )
        {
            throw new IOException( format(
                    "Failed to load certificates from file %s as you have no read permissions to it.\n" +
                    "Try configuring the Neo4j driver to use a file system location you do have read permissions to.",
                    knownHostsFile.getAbsolutePath()
            ) );
        }
    }

    private void assertKnownHostFileWritable() throws IOException
    {
        if( !knownHostsFile.canWrite() )
        {
            throw new IOException( format(
                    "Failed to write certificates to file %s as you have no write permissions to it.\n" +
                    "Try configuring the Neo4j driver to use a file system location you do have write permissions to.",
                    knownHostsFile.getAbsolutePath()
            ) );
        }
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

        String cert = fingerprint( certificate );
        String fingerprint = this.knownHosts.get( serverId );

        if ( fingerprint == null )
        {
            try
            {
                saveTrustedHost( serverId, cert );
            }
            catch ( IOException e )
            {
                throw new CertificateException( format(
                        "Failed to save the server ID and the certificate received from the server to file %s.\n" +
                        "Server ID: %s\nReceived cert:\n%s",
                        knownHostsFile.getAbsolutePath(), serverId, X509CertToString( cert ) ), e );
            }
        }
        else
        {
            if ( !fingerprint.equals( cert ) )
            {
                throw new CertificateException( format(
                        "Unable to connect to neo4j at `%s`, because the certificate the server uses has changed. " +
                        "This is a security feature to protect against man-in-the-middle attacks.\n" +
                        "If you trust the certificate the server uses now, simply remove the line that starts with " +
                        "`%s` " +
                        "in the file `%s`.\n" +
                        "The old certificate saved in file is:\n%sThe New certificate received is:\n%s",
                        serverId, serverId, knownHostsFile.getAbsolutePath(),
                        X509CertToString( fingerprint ), X509CertToString( cert ) ) );
            }
        }
    }

    /**
     * Calculate the certificate fingerprint - simply the SHA-512 hash of the DER-encoded certificate.
     */
    public static String fingerprint( X509Certificate cert ) throws CertificateException
    {
        try
        {
            MessageDigest md = MessageDigest.getInstance( "SHA-512" );
            md.update( cert.getEncoded() );
            return BytePrinter.compactHex( md.digest() );
        }
        catch( NoSuchAlgorithmException e )
        {
            // SHA-1 not available
            throw new CertificateException( "Cannot use TLS on this platform, because SHA-512 message digest algorithm is not available: " + e.getMessage(), e );
        }
    }

    private File createKnownCertFileIfNotExists() throws IOException
    {
        if ( !knownHostsFile.exists() )
        {
            File parentDir = knownHostsFile.getParentFile();
            try
            {
                if ( parentDir != null && !parentDir.exists() )
                {
                    if ( !parentDir.mkdirs() )
                    {
                        throw new IOException( "Failed to create directories for the known hosts file in " + knownHostsFile

                                .getAbsolutePath() +
                                               ". This is usually because you do not have write permissions to the directory. " +
                                               "Try configuring the Neo4j driver to use a file system location you do have write permissions to." );
                    }
                }
                if ( !knownHostsFile.createNewFile() )
                {
                    throw new IOException( "Failed to create a known hosts file at " + knownHostsFile
                            .getAbsolutePath() +
                                           ". This is usually because you do not have write permissions to the directory. " +
                                           "Try configuring the Neo4j driver to use a file system location you do have write permissions to." );
                }
            }
            catch( SecurityException e )
            {
                throw new IOException( "Failed to create known host file and/or parent directories at " + knownHostsFile
                        .getAbsolutePath() +
                                       ". This is usually because you do not have write permission to the directory. " +
                                       "Try configuring the Neo4j driver to use a file location you have write permissions to." );
            }
            BufferedWriter writer = new BufferedWriter( new FileWriter( knownHostsFile ) );
            writer.write( "# This file contains trusted certificates for Neo4j servers, it's created by Neo4j drivers." );
            writer.newLine();
            writer.write( "# You can configure the location of this file in `org.neo4j.driver.Config`" );
            writer.newLine();
            writer.close();
        }

        return knownHostsFile;
    }

    /**
     * No issuer is trusted.
     */
    public X509Certificate[] getAcceptedIssuers()
    {
        return new X509Certificate[0];
    }
}
