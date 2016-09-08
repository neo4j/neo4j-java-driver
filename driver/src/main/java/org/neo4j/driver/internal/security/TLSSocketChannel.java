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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.security.GeneralSecurityException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.internal.util.BytePrinter;
import org.neo4j.driver.internal.util.BytePrinter;
import org.neo4j.driver.v1.Config.TrustStrategy;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.exceptions.ClientException;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;

/**
 * A blocking TLS socket channel.
 *
 * When debugging, we could enable JSSE system debugging by setting system property:
 * {@code -Djavax.net.debug=all} to value more information about handshake messages and other operations underway.
 *
 * References:
 * http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#SSLENG
 * http://docs.oracle.com/javase/1.5.0/docs/guide/security/jsse/JSSERefGuide.html#SSLENG
 * http://docs.oracle.com/javase/7/docs/api/javax/net/ssl/SSLEngine.html
 */
public class TLSSocketChannel implements ByteChannel
{
    private final ByteChannel channel;      // The real channel the data is sent to and read from
    private final Logger logger;

    private SSLEngine sslEngine;

    /** The buffer for network data */
    private ByteBuffer cipherOut;
    private ByteBuffer cipherIn;
    /** The buffer for application data */
    private ByteBuffer plainIn;
    private ByteBuffer plainOut;

    private static final ByteBuffer DUMMY_BUFFER = ByteBuffer.allocate( 0 );

    public TLSSocketChannel( BoltServerAddress address, SecurityPlan securityPlan, ByteChannel channel, Logger logger )
            throws GeneralSecurityException, IOException
    {
        this( channel, logger, createSSLEngine( address, securityPlan.sslContext() ) );
    }

    public TLSSocketChannel( ByteChannel channel, Logger logger, SSLEngine sslEngine ) throws GeneralSecurityException, IOException
    {
        this(channel, logger, sslEngine,
             ByteBuffer.allocate( sslEngine.getSession().getApplicationBufferSize() ),
             ByteBuffer.allocate( sslEngine.getSession().getPacketBufferSize() ),
             ByteBuffer.allocate( sslEngine.getSession().getApplicationBufferSize() ),
             ByteBuffer.allocate( sslEngine.getSession().getPacketBufferSize() ) );
    }

    TLSSocketChannel( ByteChannel channel, Logger logger, SSLEngine sslEngine,
                      ByteBuffer plainIn, ByteBuffer cipherIn, ByteBuffer plainOut, ByteBuffer cipherOut )
            throws GeneralSecurityException, IOException
    {
        this.logger = logger;
        this.channel = channel;
        this.sslEngine = sslEngine;
        this.plainIn = plainIn;
        this.cipherIn = cipherIn;
        this.plainOut = plainOut;
        this.cipherOut = cipherOut;
        runHandshake();
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
    private void runHandshake() throws IOException
    {
        logger.debug( "~~ [OPENING SECURE CHANNEL]" );
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
                // Unwrap the ssl packet to value ssl handshake information
                handshakeStatus = unwrap( DUMMY_BUFFER );
                break;
            case NEED_WRAP:
                // Wrap the app packet into an ssl packet to add ssl handshake information
                handshakeStatus = wrap( plainOut );
                break;
            }
        }
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
     * @param buffer to read data into.
     * @return The status of the current handshake.
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

        Status status;
        do
        {
            SSLEngineResult unwrapResult = sslEngine.unwrap( cipherIn, plainIn );
            status = unwrapResult.getStatus();
            // Possible status here:
            // OK - good
            // BUFFER_OVERFLOW - we need to enlarge* plainIn
            // BUFFER_UNDERFLOW - we need to enlarge* cipherIn and read more bytes from channel

            /* In normal situations, enlarging buffers should happen very very rarely, as usually,
             the initial size for application buffer and network buffer is greater than 1024 * 10,
             and by using ChunkedInput and ChunkedOutput, the chunks are limited to 8192.
             So we should not value any enlarging buffer request in most cases. */
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
                ByteBuffer newPlainIn = ByteBuffer.allocate( newAppSize );
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
                    ByteBuffer newCipherIn = ByteBuffer.allocate( netSize );
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
                return handshakeStatus; // old status
            case CLOSED:
                // RFC 2246 #7.2.1 requires us to stop accepting input.
                sslEngine.closeInbound();
                break;
            default:
                throw new ClientException( "Got unexpected status " + status + ", " + unwrapResult );
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
     * @return The status of the current handshake
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
            while ( cipherOut.hasRemaining() )
            {
                channel.write( cipherOut );
            }
            cipherOut.clear();
            break;
        case BUFFER_OVERFLOW:
            // Enlarge the buffer and return the old status
            int curNetSize = cipherOut.capacity();
            int netSize = sslEngine.getSession().getPacketBufferSize();
            if ( netSize > curNetSize )
            {
                // enlarge the peer application data buffer
                cipherOut = ByteBuffer.allocate( netSize );
                logger.debug( "Enlarged network output buffer from %s to %s. " +
                              "This operation should be a rare operation.", curNetSize, netSize );
            }
            else
            {
                // flush as much data as possible
                cipherOut.flip();
                int written = channel.write( cipherOut );
                if (written == 0)
                {
                    throw new ClientException(
                            String.format(
                                    "Failed to enlarge network buffer from %s to %s. This is either because the " +
                                    "new size is however less than the old size, or because the application " +
                                    "buffer size %s is so big that the application data still cannot fit into the " +
                                    "new network buffer.", curNetSize, netSize, buffer.capacity() ) );
                }
                cipherOut.compact();
                logger.debug( "Network output buffer couldn't be enlarged, flushing data to the channel instead." );
            }
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
     * @param from buffer to copy from
     * @param to buffer to copy to
     * @return the number of transferred bytes
     */
    static int bufferCopy( ByteBuffer from, ByteBuffer to )
    {
        int maxTransfer = Math.min( to.remaining(), from.remaining() );

        //use a temp buffer and move all data in one go
        ByteBuffer temporaryBuffer = from.duplicate();
        temporaryBuffer.limit( temporaryBuffer.position() + maxTransfer );
        to.put( temporaryBuffer );

        //move postion so it appears as if we read the buffer
        from.position( from.position() + maxTransfer );

        return maxTransfer;
    }

    /**
     * Create SSLEngine with the SSLContext just created.
     * @param address the host to connect to
     * @param sslContext the current ssl context
     */
    private static SSLEngine createSSLEngine( BoltServerAddress address, SSLContext sslContext )
    {
        SSLEngine sslEngine = sslContext.createSSLEngine( address.host(), address.port() );
        sslEngine.setUseClientMode( true );
        return sslEngine;
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
        if ( plainIn.hasRemaining() )
        {
            bufferCopy( plainIn, dst );
            plainIn.compact();
        }
        else
        {
            plainIn.clear();            // Clear plainIn
            unwrap( dst );              // Read more data from the underline channel and save the data read into dst
        }

        return toRead - dst.remaining();
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
        try
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
                cipherOut.flip();
                while ( cipherOut.hasRemaining() )
                {
                    int num = channel.write( cipherOut );
                    if ( num == -1 )
                    {
                        // handle closed channel
                        break;
                    }
                }
                cipherOut.clear();
            }
            // Close transport
            channel.close();
            logger.debug( "~~ [CLOSED SECURE CHANNEL]" );
        }
        catch ( IOException e )
        {
            // Treat this as ok - the connection is closed, even if the TLS session did not exit cleanly.
            logger.warn( "TLS socket could not be closed cleanly: '" + e.getMessage() + "'", e );
        }
    }

    @Override
    public String toString()
    {
        return "TLSSocketChannel{plainIn: " + plainIn + ", cipherIn:" + cipherIn + "}";
    }
}
