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
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLSession;

import org.neo4j.driver.v1.Config.TlsAuthenticationConfig;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.internal.spi.Logger;
import org.neo4j.driver.v1.internal.util.BytePrinter;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;

/**
 * A blocking SSL socket channel.
 *
 * When debugging, we could enable JSSE system debugging by setting system property:
 * {@code -Djavax.net.debug=all} to get more information about handshake messages and other operations underway.
 *
 * References:
 * http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#SSLENG
 * http://docs.oracle.com/javase/1.5.0/docs/guide/security/jsse/JSSERefGuide.html#SSLENG
 * http://docs.oracle.com/javase/7/docs/api/javax/net/ssl/SSLEngine.html
 */
public class SSLSocketChannel implements ByteChannel
{
    private final SocketChannel channel;      // The real channel the data is sent to and read from
    private final Logger logger;

    private final SSLContext sslContext;
    private SSLEngine sslEngine;

    /** The buffer for network data */
    private ByteBuffer cipherOut;
    private ByteBuffer cipherIn;
    /** The buffer for application data */
    private ByteBuffer plainIn;
    private ByteBuffer plainOut;

    public SSLSocketChannel( String host, int port, SocketChannel channel, Logger logger,
            TlsAuthenticationConfig authConfig )
            throws GeneralSecurityException, IOException
    {
        logger.debug( "TLS connection enabled" );
        this.logger = logger;
        this.channel = channel;
        this.channel.configureBlocking( true );

        sslContext =  new SSLContextFactory( host, port, authConfig ).create();
        createSSLEngine( host, port );
        createBuffers();
        runSSLHandShake();
        logger.debug( "TLS connection established" );
    }

    /** Used in internal tests only */
    SSLSocketChannel( SocketChannel channel, Logger logger, SSLEngine sslEngine,
            ByteBuffer plainIn, ByteBuffer cipherIn, ByteBuffer plainOut, ByteBuffer cipherOut )
            throws GeneralSecurityException, IOException
    {
        logger.debug( "Testing TLS buffers" );
        this.logger = logger;
        this.channel = channel;

        this.sslContext = SSLContext.getInstance( "TLS" );
        this.sslEngine = sslEngine;
        resetBuffers( plainIn, cipherIn, plainOut, cipherOut ); // reset buffer size
    }

    /**
     * A typical handshake on the client side might looks like:
     * <table>
     * <tr><td><b>Client</b></td><td><b>SSL/TLS message</b></td>          <td><b>HSStatus</b></td></tr>
     * <tr><td>wrap()</td>          <td>ClientHello</td>                     <td>NEED_UNWRAP</td></tr>
     * <tr><td>unwrap()</td>        <td>ServerHello/Cert/ServerHelloDone</td><td>NEED_WRAP</td></tr>
     * <tr><td>wrap()</td>          <td>ClientKeyExchange</td>               <td>NEED_WRAP</td></tr>
     * <tr><td>wrap()</td>          <td>ChangeCipherSpec</td>                <td>NEED_WRAP</td></tr>
     * <tr><td>wrap()</td>          <td>Finished</td>                        <td>NEED_UNWRAP</td></tr>
     * <tr><td>unwrap()</td>        <td>ChangeCipherSpec</td>                <td>NEED_UNWRAP</td></tr>
     * <tr><td>unwrap()</td>        <td>Finished</td>                        <td>FINISHED</td></tr>
     * </table>
     *
     * @throws IOException
     */
    private void runSSLHandShake() throws IOException
    {
        sslEngine.beginHandshake();
        HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
        while ( handshakeStatus != FINISHED && handshakeStatus != NOT_HANDSHAKING )
        {
            switch ( handshakeStatus )
            {
            case NEED_TASK:
                // Do the delegate task if there is some extra work such as checking the keystore during the handshake
                handshakeStatus = runDelegatedTasks();
                break;
            case NEED_UNWRAP:
                // Unwrap the ssl packet to get ssl handshake information
                handshakeStatus = unwrap( null );
                plainIn.clear();
                break;
            case NEED_WRAP:
                // Wrap the app packet into an ssl packet to add ssl handshake information
                handshakeStatus = wrap( plainOut );
                break;
            }
        }

        plainIn.clear();
        plainOut.clear();
    }

    private HandshakeStatus runDelegatedTasks()
    {
        Runnable runnable;
        while ( (runnable = sslEngine.getDelegatedTask()) != null )
        {
            runnable.run();
        }
        return sslEngine.getHandshakeStatus();
    }

    /**
     * This method is mainly responsible for reading encrypted bytes from underlying socket channel and deciphering
     * them.
     *
     * These deciphered data would be saved into {@code buffer} if it is specified (not null) and if its size is
     * greater than the size of deciphered data.
     * Otherwise, the deciphered bytes or the bytes that could not fit into {@code buffer} would be left in {@code
     * plainIn} buffer.
     *
     * If the byes in {@code plaintIn} will not be used outside this method, we should always clear {@code
     * plainIn} after each call to avoid wasting memory on it.
     * <p>
     * When working with this method, we should always put this method in a loop as there is no guarantee
     * that deciphering would be carried out successfully in each call. The possible reasons that deciphering fails
     * in a call could be requiring more bytes to decipher, too small buffers to hold all data, etc.
     *
     * To verify if deciphering is done successfully, we could check if any bytes has been read into {@code buffer},
     * as the deciphered bytes will only be saved into {@code buffer} when deciphering is carried out successfully.
     *
     * @param buffer
     * @return
     * @throws IOException
     */
    private HandshakeStatus unwrap( ByteBuffer buffer ) throws IOException
    {
        HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();

        /**
         * This is the only place to read from the underlying channel
         */
        if ( channel.read( cipherIn ) < 0 )
        {
            throw new ClientException( "Failed to establish SSL socket connection." );
        }
        cipherIn.flip();

        Status status = null;
        do
        {
            status = sslEngine.unwrap( cipherIn, plainIn ).getStatus();
            // Possible status here:
            // OK - good
            // BUFFER_OVERFLOW - we need to enlarge* plainIn
            // BUFFER_UNDERFLOW - we need to enlarge* cipherIn and read more bytes from channel

            /* In normal situations, enlarging buffers should happen very very rarely, as usually,
             the initial size for application buffer and network buffer is greater than 1024 * 10,
             and by using ChunkedInput and ChunkedOutput, the chunks are limited to 8192.
             So we should not get any enlarging buffer request in most cases. */
            switch ( status )
            {
            case OK:
                plainIn.flip();
                bufferCopy( plainIn, buffer );
                plainIn.compact();
                handshakeStatus = runDelegatedTasks();
                break;
            case BUFFER_OVERFLOW:
                plainIn.flip();
                // Could attempt to drain the plainIn buffer of any already obtained
                // data, but we'll just increase it to the size needed.
                int curAppSize = plainIn.capacity();
                int appSize = sslEngine.getSession().getApplicationBufferSize();
                int newAppSize = appSize + plainIn.remaining();
                if ( newAppSize > appSize * 2 )
                {
                    throw new ClientException(
                            String.format( "Failed ro enlarge application input buffer from %s to %s, as the maximum " +
                                           "buffer size allowed is %s. The content in the buffer is: %s\n",
                                    curAppSize, newAppSize, appSize * 2, BytePrinter.hex( plainIn ) ) );
                }
                ByteBuffer newPlainIn = ByteBuffer.allocateDirect( newAppSize );
                newPlainIn.put( plainIn );
                plainIn = newPlainIn;
                logger.debug( "Enlarged application input buffer from %s to %s. " +
                              "This operation should be a rare operation.", curAppSize, newAppSize );
                // retry the operation.
                break;
            case BUFFER_UNDERFLOW:
                int curNetSize = cipherIn.capacity();
                int netSize = sslEngine.getSession().getPacketBufferSize();
                // Resize buffer if needed.
                if ( netSize > curNetSize )
                {
                    ByteBuffer newCipherIn = ByteBuffer.allocateDirect( netSize );
                    newCipherIn.put( cipherIn );
                    cipherIn = newCipherIn;
                    logger.debug( "Enlarged network input buffer from %s to %s. " +
                                  "This operation should be a rare operation.", curNetSize, netSize );
                }
                else
                {
                    // Otherwise, make room for reading more data from channel
                    cipherIn.compact();
                }

                // I skipped the following check as it "should not" happen at all:
                // The channel should not provide us ciphered bytes that cannot hold in the channel buffer at all
                // if( cipherIn.remaining() == 0 )
                // {throw new ClientException( "cannot enlarge as it already reached the limit" );}

                // Obtain more inbound network data for cipherIn,
                // then retry the operation.
                return handshakeStatus; // old status
            default:
                throw new ClientException( "Got unexpected status " + status );
            }
        }
        while ( cipherIn.hasRemaining() ); /* Remember we are doing blocking reading.
        We expect we should first handle all the data we've got and then decide what to do next. */

        cipherIn.compact();
        return handshakeStatus;
    }

    /**
     * Encrypt the bytes given in {@code buffer} and write them out to the channel, when using this method, put it in
     * a loop
     *
     * @param buffer contains the bytes to send to channel
     * @return
     * @throws IOException
     */
    private HandshakeStatus wrap( ByteBuffer buffer ) throws IOException
    {
        HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
        Status status = sslEngine.wrap( buffer, cipherOut ).getStatus();
        // Possible status here:
        // Ok - good
        // BUFFER_OVERFLOW - we need to enlarge cipherOut to hold all ciphered data (should happen very rare)
        // BUFFER_UNDERFLOW - we need to enlarge buffer (shouldn't happen)
        switch ( status )
        {
        case OK:
            handshakeStatus = runDelegatedTasks();
            cipherOut.flip();
            channel.write( cipherOut );
            cipherOut.clear();
            break;
        case BUFFER_OVERFLOW:
            // Enlarge the buffer and return the old status
            int curNetSize = cipherOut.capacity();
            int netSize = sslEngine.getSession().getPacketBufferSize();
            if ( curNetSize >= netSize || buffer.capacity() > netSize )
            {
                // TODO
                throw new ClientException(
                        String.format( "Failed to enlarge network buffer from %s to %s. This is either because the " +
                                       "new size is however less than the old size, or because the application " +
                                       "buffer size %s is so big that the application data still cannot fit into the " +
                                       "new network buffer.", curNetSize, netSize, buffer.capacity() ) );
            }

            cipherOut = ByteBuffer.allocateDirect( netSize );
            logger.debug( "Enlarged network output buffer from %s to %s. " +
                          "This operation should be a rare operation.", curNetSize, netSize );
            break;
        default:
            throw new ClientException( "Got unexpected status " + status );
        }
        return handshakeStatus;
    }

    /**
     * Copy the buffer content from one buffer to another.
     * <pre>
     * from: 0--------pos-----------------limit------cap
     *                 |-data to be copied-|
     * to:   0----pos------------------------limit---cap
     *             |-data will be append after
     * </pre>
     * If {@code n = to.limit - to.pos, m = from.limit - from.pos}, then the amount of data will be copied
     * {@code p = min(n, m)}.
     * After the method call, the new position of {@code from.pos} will be {@code from.pos + p}, and similarly,
     * the new position of {@code to.pos} will be {@code to.pos + p}
     *
     * @param from
     * @param to
     * @return
     */
    static int bufferCopy( ByteBuffer from, ByteBuffer to )
    {
        if ( from == null || to == null )
        {
            return 0;
        }

        int i;
        for ( i = 0; to.remaining() > 0 && from.remaining() > 0; i++ )
        {
            to.put( from.get() );
        }
        return i;
    }

    /**
     * Create network buffers and application buffers
     *
     * @throws IOException
     */
    private void createBuffers() throws IOException
    {
        SSLSession session = sslEngine.getSession();
        int appBufferSize = session.getApplicationBufferSize();
        int netBufferSize = session.getPacketBufferSize();

        plainOut = ByteBuffer.allocateDirect( appBufferSize );
        plainIn = ByteBuffer.allocateDirect( appBufferSize );
        cipherOut = ByteBuffer.allocateDirect( netBufferSize );
        cipherIn = ByteBuffer.allocateDirect( netBufferSize );
    }

    /** Should only be used in tests */
    void resetBuffers( ByteBuffer plainIn, ByteBuffer cipherIn, ByteBuffer plainOut, ByteBuffer cipherOut )
    {
        this.plainIn = plainIn;
        this.cipherIn = cipherIn;
        this.plainOut = plainOut;
        this.cipherOut = cipherOut;
    }

    /**
     * Create SSLEngine with the SSLContext just created.
     *
     * @param host
     * @param port
     */
    private void createSSLEngine( String host, int port )
    {
        sslEngine = sslContext.createSSLEngine( host, port );
        sslEngine.setUseClientMode( true );
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        /**
         * First try to read from plainBuffer.
         * If not enough bytes left in plainBuffer,
         * read encrypted data from underlying channel and put the deciphered data in the plain buffer.
         * Return how many deciphered data that have been put dst.
         */
        int toRead = dst.remaining();
        plainIn.flip();
        if ( plainIn.remaining() >= toRead )
        {
            bufferCopy( plainIn, dst );
            plainIn.compact();
        }
        else
        {
            dst.put( plainIn );             // Copy whatever left in the plainIn to dst
            do
            {
                plainIn.clear();            // Clear plainIn
                unwrap( dst );              // Read more data from the underline channel and save the data read into dst
            }
            while ( dst.remaining() > 0 );  // If enough bytes read then return otherwise continue reading from channel
        }

        return toRead;
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        /**
         * Encrypt the plain text data in src buffer and write them into underlying channel.
         * Return how many plain text data in src that have been written to the underlying channel.
         */
        int toWrite = src.remaining();
        while ( src.remaining() > 0 )
        {
            wrap( src );
        }
        return toWrite;
    }

    @Override
    public boolean isOpen()
    {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException
    {
        plainOut.clear();
        // Indicate that application is done with engine
        sslEngine.closeOutbound();

        while ( !sslEngine.isOutboundDone() )
        {
            // Get close message
            SSLEngineResult res = sslEngine.wrap( plainOut, cipherOut );

            // Check res statuses

            // Send close message to peer
            while ( cipherOut.hasRemaining() )
            {
                int num = channel.write( cipherOut );
                if ( num == -1 )
                {
                    // handle closed channel
                    break;
                }
            }
        }
        // Close transport
        channel.close();
        logger.debug( "TLS connection closed" );
    }

}
