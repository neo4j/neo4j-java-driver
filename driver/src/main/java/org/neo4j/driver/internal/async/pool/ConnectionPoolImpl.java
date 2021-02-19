/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.internal.async.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.metrics.ListenerEvent;
import org.neo4j.driver.internal.metrics.MetricsListener;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Futures;

import static java.lang.String.format;
import static org.neo4j.driver.internal.util.Futures.combineErrors;
import static org.neo4j.driver.internal.util.Futures.completeWithNullIfNoError;

public class ConnectionPoolImpl implements ConnectionPool
{
    private final ChannelConnector connector;
    private final Bootstrap bootstrap;
    private final NettyChannelTracker nettyChannelTracker;
    private final NettyChannelHealthChecker channelHealthChecker;
    private final PoolSettings settings;
    private final Logger log;
    private final MetricsListener metricsListener;
    private final boolean ownsEventLoopGroup;

    private final ConcurrentMap<BoltServerAddress,ExtendedChannelPool> pools = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private final ConnectionFactory connectionFactory;

    public ConnectionPoolImpl( ChannelConnector connector, Bootstrap bootstrap, PoolSettings settings, MetricsListener metricsListener, Logging logging,
            Clock clock, boolean ownsEventLoopGroup )
    {
        this( connector, bootstrap, new NettyChannelTracker( metricsListener, bootstrap.config().group().next(), logging ), settings, metricsListener, logging,
                clock, ownsEventLoopGroup, new NetworkConnectionFactory( clock, metricsListener ) );
    }

    public ConnectionPoolImpl( ChannelConnector connector, Bootstrap bootstrap, NettyChannelTracker nettyChannelTracker, PoolSettings settings,
            MetricsListener metricsListener, Logging logging, Clock clock, boolean ownsEventLoopGroup, ConnectionFactory connectionFactory )
    {
        this.connector = connector;
        this.bootstrap = bootstrap;
        this.nettyChannelTracker = nettyChannelTracker;
        this.channelHealthChecker = new NettyChannelHealthChecker( settings, clock, logging );
        this.settings = settings;
        this.metricsListener = metricsListener;
        this.log = logging.getLog( ConnectionPool.class.getSimpleName() );
        this.ownsEventLoopGroup = ownsEventLoopGroup;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public CompletionStage<Connection> acquire( BoltServerAddress address )
    {
        log.trace( "Acquiring a connection from pool towards %s", address );

        assertNotClosed();
        ExtendedChannelPool pool = getOrCreatePool( address );

        ListenerEvent acquireEvent = metricsListener.createListenerEvent();
        metricsListener.beforeAcquiringOrCreating( pool.id(), acquireEvent );
        CompletionStage<Channel> channelFuture = pool.acquire();

        return channelFuture.handle( ( channel, error ) ->
        {
            try
            {
                processAcquisitionError( pool, address, error );
                assertNotClosed( address, channel, pool );
                Connection connection = connectionFactory.createConnection( channel, pool );

                metricsListener.afterAcquiredOrCreated( pool.id(), acquireEvent );
                return connection;
            }
            finally
            {
                metricsListener.afterAcquiringOrCreating( pool.id() );
            }
        } );
    }

    @Override
    public void retainAll( Set<BoltServerAddress> addressesToRetain )
    {
        for ( BoltServerAddress address : pools.keySet() )
        {
            if ( !addressesToRetain.contains( address ) )
            {
                int activeChannels = nettyChannelTracker.inUseChannelCount( address );
                if ( activeChannels == 0 )
                {
                    // address is not present in updated routing table and has no active connections
                    // it's now safe to terminate corresponding connection pool and forget about it
                    ExtendedChannelPool pool = pools.remove( address );
                    if ( pool != null )
                    {
                        log.info( "Closing connection pool towards %s, it has no active connections " +
                                  "and is not in the routing table registry.", address );
                        closePoolInBackground( address, pool );
                    }
                }
            }
        }
    }

    @Override
    public int inUseConnections( BoltServerAddress address )
    {
        return nettyChannelTracker.inUseChannelCount( address );
    }

    @Override
    public int idleConnections( BoltServerAddress address )
    {
        return nettyChannelTracker.idleChannelCount( address );
    }

    @Override
    public CompletionStage<Void> close()
    {
        if ( closed.compareAndSet( false, true ) )
        {
            nettyChannelTracker.prepareToCloseChannels();
            CompletableFuture<Void> allPoolClosedFuture = closeAllPools();

            // We can only shutdown event loop group when all netty pools are fully closed,
            // otherwise the netty pools might missing threads (from event loop group) to execute clean ups.
            allPoolClosedFuture.whenComplete( ( ignored, pollCloseError ) -> {
                pools.clear();
                if ( !ownsEventLoopGroup )
                {
                    completeWithNullIfNoError( closeFuture, pollCloseError );
                }
                else
                {
                    shutdownEventLoopGroup( pollCloseError );
                }
            } );
        }
        return closeFuture;
    }

    @Override
    public boolean isOpen( BoltServerAddress address )
    {
        return pools.containsKey( address );
    }

    @Override
    public String toString()
    {
        return "ConnectionPoolImpl{" + "pools=" + pools + '}';
    }

    private void processAcquisitionError( ExtendedChannelPool pool, BoltServerAddress serverAddress, Throwable error )
    {
        Throwable cause = Futures.completionExceptionCause( error );
        if ( cause != null )
        {
            if ( cause instanceof TimeoutException )
            {
                // NettyChannelPool returns future failed with TimeoutException if acquire operation takes more than
                // configured time, translate this exception to a prettier one and re-throw
                metricsListener.afterTimedOutToAcquireOrCreate( pool.id() );
                throw new ClientException(
                        "Unable to acquire connection from the pool within configured maximum time of " +
                        settings.connectionAcquisitionTimeout() + "ms" );
            }
            else if ( pool.isClosed() )
            {
                // There is a race condition where a thread tries to acquire a connection while the pool is closed by another concurrent thread.
                // Treat as failed to obtain connection for a direct driver. For a routing driver, this error should be retried.
                throw new ServiceUnavailableException( format( "Connection pool for server %s is closed while acquiring a connection.", serverAddress ),
                        cause );
            }
            else
            {
                // some unknown error happened during connection acquisition, propagate it
                throw new CompletionException( cause );
            }
        }
    }

    private void assertNotClosed()
    {
        if ( closed.get() )
        {
            throw new IllegalStateException( "Pool closed" );
        }
    }

    private void assertNotClosed( BoltServerAddress address, Channel channel, ExtendedChannelPool pool )
    {
        if ( closed.get() )
        {
            pool.release( channel );
            closePoolInBackground( address, pool );
            pools.remove( address );
            assertNotClosed();
        }
    }

    // for testing only
    ExtendedChannelPool getPool( BoltServerAddress address )
    {
        return pools.get( address );
    }

    ExtendedChannelPool newPool( BoltServerAddress address )
    {
        return new NettyChannelPool( address, connector, bootstrap, nettyChannelTracker, channelHealthChecker, settings.connectionAcquisitionTimeout(),
                settings.maxConnectionPoolSize() );
    }

    private ExtendedChannelPool getOrCreatePool( BoltServerAddress address )
    {
        return pools.computeIfAbsent( address, ignored -> {
            ExtendedChannelPool pool = newPool( address );
            // before the connection pool is added I can add the metrics for the pool.
            metricsListener.putPoolMetrics( pool.id(), address, this );
            return pool;
        } );
    }

    private CompletionStage<Void> closePool( ExtendedChannelPool pool )
    {
        return pool.close().whenComplete( ( ignored, error ) ->
                // after the connection pool is removed/close, I can remove its metrics.
                metricsListener.removePoolMetrics( pool.id() ) );
    }

    private void closePoolInBackground( BoltServerAddress address, ExtendedChannelPool pool )
    {
        // Close in the background
        closePool( pool ).whenComplete( ( ignored, error ) -> {
            if ( error != null )
            {
                log.warn( format( "An error occurred while closing connection pool towards %s.", address ), error );
            }
        } );
    }

    private EventLoopGroup eventLoopGroup()
    {
        return bootstrap.config().group();
    }

    private void shutdownEventLoopGroup( Throwable pollCloseError )
    {
        // This is an attempt to speed up the shut down procedure of the driver
        // This timeout is needed for `closePoolInBackground` to finish background job, especially for races between `acquire` and `close`.
        eventLoopGroup().shutdownGracefully( 200, 15_000, TimeUnit.MILLISECONDS );

        Futures.asCompletionStage( eventLoopGroup().terminationFuture() )
                .whenComplete( ( ignore, eventLoopGroupTerminationError ) -> {
                    CompletionException combinedErrors = combineErrors( pollCloseError, eventLoopGroupTerminationError );
                    completeWithNullIfNoError( closeFuture, combinedErrors );
                } );
    }

    private CompletableFuture<Void> closeAllPools()
    {
        return CompletableFuture.allOf(
                pools.entrySet().stream().map( entry -> {
                    BoltServerAddress address = entry.getKey();
                    ExtendedChannelPool pool = entry.getValue();
                    log.info( "Closing connection pool towards %s", address );
                    // Wait for all pools to be closed.
                    return closePool( pool ).toCompletableFuture();
                } ).toArray( CompletableFuture[]::new ) );
    }
}
