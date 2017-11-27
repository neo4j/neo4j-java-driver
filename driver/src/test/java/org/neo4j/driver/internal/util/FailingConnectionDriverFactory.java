/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.util;

import io.netty.bootstrap.Bootstrap;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Value;

public class FailingConnectionDriverFactory extends DriverFactory
{
    private final AtomicReference<Throwable> nextRunFailure = new AtomicReference<>();

    @Override
    protected ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan, Bootstrap bootstrap,
            Config config )
    {
        ConnectionPool pool = super.createConnectionPool( authToken, securityPlan, bootstrap, config );
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
        public int activeConnections( BoltServerAddress address )
        {
            return delegate.activeConnections( address );
        }

        @Override
        public CompletionStage<Void> close()
        {
            return delegate.close();
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
        public void run( String statement, Map<String,Value> parameters, ResponseHandler runHandler,
                ResponseHandler pullAllHandler )
        {
            if ( tryFail( runHandler, pullAllHandler ) )
            {
                return;
            }
            delegate.run( statement, parameters, runHandler, pullAllHandler );
        }

        @Override
        public void runAndFlush( String statement, Map<String,Value> parameters, ResponseHandler runHandler,
                ResponseHandler pullAllHandler )
        {
            if ( tryFail( runHandler, pullAllHandler ) )
            {
                return;
            }
            delegate.runAndFlush( statement, parameters, runHandler, pullAllHandler );
        }

        @Override
        public CompletionStage<Void> release()
        {
            return delegate.release();
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

        private boolean tryFail( ResponseHandler runHandler, ResponseHandler pullAllHandler )
        {
            Throwable failure = nextRunFailure.getAndSet( null );
            if ( failure != null )
            {
                runHandler.onFailure( failure );
                pullAllHandler.onFailure( failure );
                return true;
            }
            return false;
        }
    }
}

