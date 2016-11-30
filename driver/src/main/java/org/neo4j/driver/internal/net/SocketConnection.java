/*
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
package org.neo4j.driver.internal.net;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.exceptions.BoltProtocolException;
import org.neo4j.driver.internal.exceptions.ServerNeo4jException;
import org.neo4j.driver.internal.exceptions.InvalidOperationException;
import org.neo4j.driver.internal.messaging.InitMessage;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.exceptions.ConnectionException;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.ServerInfo;

import static java.lang.String.format;
import static org.neo4j.driver.internal.messaging.AckFailureMessage.ACK_FAILURE;
import static org.neo4j.driver.internal.messaging.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.internal.messaging.ResetMessage.RESET;

public class SocketConnection implements Connection
{
    private final Queue<Message> pendingMessages = new LinkedList<>();
    private final SocketResponseHandler responseHandler;

    /** When interrupted, before queuing more messages, unhandled failure messages should be consumed first.
     * This ensures that before running more statements after a client reset, no unhandled error is left in the
     * message queue.
     * If the connection {@link #isInterrupted}, then it definitely also {@link #isAckFailureMuted}*/
    private final AtomicBoolean isInterrupted = new AtomicBoolean( false );

    /** When ackFailure is muted, calling {@link #ackFailure()} will put no ackFailure message into the sending
     * message queue, as the error will be cleaned by client's reset instead.
     * {@link #isAckFailureMuted} will only be set back to false when a success of reset message is received. */
    private final AtomicBoolean isAckFailureMuted = new AtomicBoolean( false );

    private InternalServerInfo serverInfo;

    private final SocketClient socket;
    private final Logger logger;

    public SocketConnection( BoltServerAddress address, SecurityPlan securityPlan, Logging logging )
            throws ConnectionException, InvalidOperationException
    {
        this.logger = logging.getLog( format( "conn-%s", UUID.randomUUID().toString() ) );
        this.socket = new SocketClient( address, securityPlan, logger );
        this.responseHandler = createResponseHandler( logger );
        this.socket.start();
    }

    /**
     * Create new connection backed by the given socket.
     * <p>
     * <b>Note:</b> this constructor should be used <b>only</b> for testing.
     *
     * @param socket the socket to use for network interactions.
     * @param serverInfo the info about server this connection points to.
     * @param logger the logger.
     */
    public SocketConnection( SocketClient socket, InternalServerInfo serverInfo, Logger logger )
            throws ConnectionException, InvalidOperationException
    {
        this.socket = socket;
        this.serverInfo = serverInfo;
        this.logger = logger;
        this.responseHandler = createResponseHandler( logger );
        this.socket.start();
    }

    private SocketResponseHandler createResponseHandler( Logger logger )
    {
        if( logger.isDebugEnabled() )
        {
            return new LoggingResponseHandler( logger );
        }
        else
        {
            return new SocketResponseHandler();
        }
    }

    @Override
    public void init( String clientName, Map<String,Value> authToken )
            throws ConnectionException, ServerNeo4jException, InvalidOperationException, BoltProtocolException
    {
        Collector.InitCollector initCollector = new Collector.InitCollector();
        queueMessageWithInterruptCheck( new InitMessage( clientName, authToken ), initCollector );
        sync();
        this.serverInfo = new InternalServerInfo( socket.address(), initCollector.serverVersion() );
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, Collector collector )
            throws InvalidOperationException, BoltProtocolException
    {
        queueMessageWithInterruptCheck( new RunMessage( statement, parameters ), collector );
    }

    @Override
    public void discardAll( Collector collector ) throws InvalidOperationException, BoltProtocolException
    {
        queueMessageWithInterruptCheck( DISCARD_ALL, collector );
    }

    @Override
    public void pullAll( Collector collector ) throws InvalidOperationException, BoltProtocolException
    {
        queueMessageWithInterruptCheck( PULL_ALL, collector );
    }

    @Override
    public void reset() throws InvalidOperationException, BoltProtocolException
    {
        queueMessageWithInterruptCheck( RESET, Collector.RESET );
    }

    @Override
    public synchronized void ackFailure()
    {
        // only ack failure if the ackFailure is not muted.
        if( !isAckFailureMuted.get() )
        {
            queueMessage( ACK_FAILURE, Collector.ACK_FAILURE );
        }
    }

    @Override
    public void sync()
            throws ConnectionException, ServerNeo4jException, InvalidOperationException, BoltProtocolException
    {
        flush();
        receiveAll();
    }

    @Override
    public synchronized void flush() throws ConnectionException, InvalidOperationException, BoltProtocolException
    {
        ensureNotInterrupted();

        try
        {
            socket.send( pendingMessages );
        }
        catch ( IOException e )
        {
            String message = e.getMessage();
            throw new ConnectionException( "Unable to send messages to server: " + message, e );
        }
    }

    private void ensureNotInterrupted() throws InvalidOperationException, BoltProtocolException
    {
        try
        {
            if( isInterrupted.get() )
            {
                // receive each of it and throw error immediately
                while ( responseHandler.collectorsWaiting() > 0 )
                {
                    receiveOne();
                }
            }
        }
        catch ( ServerNeo4jException | ConnectionException e )
        {
            throw new InvalidOperationException(
                    "An error has occurred due to the cancellation of executing a previous statement. " +
                    "You received this error probably because you did not consume the result immediately after " +
                    "running the statement which get reset in this session.", e );
        }

    }

    private void receiveAll() throws ConnectionException, ServerNeo4jException, BoltProtocolException
    {
        try
        {
            socket.receiveAll( responseHandler );
            assertNoServerFailure();
        }
        catch ( IOException e )
        {
            throw mapIOError( e );
        }
    }

    @Override
    public void receiveOne() throws ConnectionException, ServerNeo4jException, BoltProtocolException
    {
        try
        {
            socket.receiveOne( responseHandler );
            assertNoServerFailure();
        }
        catch ( IOException e )
        {
            throw mapIOError( e );
        }
    }

    private void assertNoServerFailure() throws ServerNeo4jException
    {
        if ( responseHandler.serverFailureOccurred() )
        {
            ServerNeo4jException exception = responseHandler.serverFailure();
            responseHandler.clearError();
            isInterrupted.set( false );
            throw exception;
        }
    }

    private ConnectionException mapIOError( IOException e )
    {
        String message = e.getMessage();
        if ( message == null )
        {
            return new ConnectionException( "Unable to read response from server: " + e.getClass().getSimpleName(), e );
        }
        else if ( e instanceof SocketTimeoutException )
        {
            return new ConnectionException( "Server did not reply within the network timeout limit.", e );
        }
        else
        {
            return new ConnectionException( "Unable to read response from server: " + message, e );
        }
    }

    private synchronized void queueMessageWithInterruptCheck( Message msg, Collector collector )
            throws InvalidOperationException, BoltProtocolException
    {
        ensureNotInterrupted();
        queueMessage( msg, collector );
    }

    private synchronized void queueMessage( Message msg, Collector collector )
    {
        pendingMessages.add( msg );
        responseHandler.appendResultCollector( collector );
    }

    @Override
    public void close() throws InvalidOperationException
    {
        try
        {
            socket.stop();
        }
        catch ( IOException e )
        {
            throw new InvalidOperationException( e.getMessage(), e );
        }
    }

    @Override
    public boolean isOpen()
    {
        return socket.isOpen();
    }

    @Override
    public synchronized void resetAsync() throws InvalidOperationException, ConnectionException, BoltProtocolException
    {
        queueMessageWithInterruptCheck( RESET, new Collector.ResetCollector( new Runnable()
        {
            @Override
            public void run()
            {
                isInterrupted.set( false );
                isAckFailureMuted.set( false );
            }
        } ) );
        flush();
        isInterrupted.set( true );
        isAckFailureMuted.set( true );
    }

    @Override
    public ServerInfo server()
    {
        return this.serverInfo;
    }

    @Override
    public BoltServerAddress boltServerAddress()
    {
        return this.serverInfo.boltServerAddress();
    }

    @Override
    public Logger logger()
    {
        return this.logger;
    }
}
