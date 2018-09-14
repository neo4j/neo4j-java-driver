/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.internal.util;

import io.netty.bootstrap.Bootstrap;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.metrics.MetricsListener;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;

public class FailingConnectionDriverFactory extends DriverFactory
{
    private final AtomicReference<Throwable> nextRunFailure = new AtomicReference<>();

    @Override
    protected ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan, Bootstrap bootstrap, EventExecutorGroup eventExecutorGroup,
            MetricsListener metrics, Config config )
    {
        ConnectionPool pool = super.createConnectionPool( authToken, securityPlan, bootstrap, eventExecutorGroup, metrics, config );
        return new ConnectionPoolWithFailingConnections( pool, nextRunFailure );
    }

    public void setNextRunFailure( Throwable failure )
    {
        nextRunFailure.set( failure );
    }

    private static class ConnectionPoolWithFailingConnections implements ConnectionPool
    {
        final ConnectionPool delegate;
        final AtomicReference<Throwable> nextRunFailure;

        ConnectionPoolWithFailingConnections( ConnectionPool delegate, AtomicReference<Throwable> nextRunFailure )
        {
            this.delegate = delegate;
            this.nextRunFailure = nextRunFailure;
        }

        @Override
        public CompletionStage<Connection> acquire( BoltServerAddress address )
        {
            return delegate.acquire( address )
                    .thenApply( connection -> new FailingConnection( connection, nextRunFailure ) );
        }

        @Override
        public void retainAll( Set<BoltServerAddress> addressesToRetain )
        {
            delegate.retainAll( addressesToRetain );
        }

        @Override
        public int inUseConnections( BoltServerAddress address )
        {
            return delegate.inUseConnections( address );
        }

        @Override
        public int idleConnections( BoltServerAddress address )
        {
            return delegate.idleConnections( address );
        }

        @Override
        public CompletionStage<Void> close()
        {
            return delegate.close();
        }

        @Override
        public boolean isOpen( BoltServerAddress address )
        {
            return delegate.isOpen( address );
        }
    }

    private static class FailingConnection implements Connection
    {
        final Connection delegate;
        final AtomicReference<Throwable> nextRunFailure;

        FailingConnection( Connection delegate, AtomicReference<Throwable> nextRunFailure )
        {
            this.delegate = delegate;
            this.nextRunFailure = nextRunFailure;
        }

        @Override
        public boolean isOpen()
        {
            return delegate.isOpen();
        }

        @Override
        public void enableAutoRead()
        {
            delegate.enableAutoRead();
        }

        @Override
        public void disableAutoRead()
        {
            delegate.disableAutoRead();
        }

        @Override
        public void write( Message message, ResponseHandler handler )
        {
            if ( tryFail( handler, null ) )
            {
                return;
            }
            delegate.write( message, handler );
        }

        @Override
        public void write( Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2 )
        {
            if ( tryFail( handler1, handler2 ) )
            {
                return;
            }
            delegate.write( message1, handler1, message2, handler2 );
        }

        @Override
        public void writeAndFlush( Message message, ResponseHandler handler )
        {
            if ( tryFail( handler, null ) )
            {
                return;
            }
            delegate.writeAndFlush( message, handler );
        }

        @Override
        public void writeAndFlush( Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2 )
        {
            if ( tryFail( handler1, handler2 ) )
            {
                return;
            }
            delegate.writeAndFlush( message1, handler1, message2, handler2 );
        }

        @Override
        public CompletionStage<Void> reset()
        {
            return delegate.reset();
        }

        @Override
        public CompletionStage<Void> release()
        {
            return delegate.release();
        }

        @Override
        public void terminateAndRelease( String reason )
        {
            delegate.terminateAndRelease( reason );
        }

        @Override
        public BoltServerAddress serverAddress()
        {
            return delegate.serverAddress();
        }

        @Override
        public ServerVersion serverVersion()
        {
            return delegate.serverVersion();
        }

        @Override
        public BoltProtocol protocol()
        {
            return delegate.protocol();
        }

        private boolean tryFail( ResponseHandler handler1, ResponseHandler handler2 )
        {
            Throwable failure = nextRunFailure.getAndSet( null );
            if ( failure != null )
            {
                handler1.onFailure( failure );
                if ( handler2 != null )
                {
                    handler2.onFailure( failure );
                }
                return true;
            }
            return false;
        }
    }
}

