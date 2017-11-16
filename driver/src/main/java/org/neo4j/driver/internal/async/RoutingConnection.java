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
package org.neo4j.driver.internal.async;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.RoutingErrorHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Value;

public class RoutingConnection implements Connection
{
    private final Connection delegate;
    private final AccessMode accessMode;
    private final RoutingErrorHandler errorHandler;

    public RoutingConnection( Connection delegate, AccessMode accessMode, RoutingErrorHandler errorHandler )
    {
        this.delegate = delegate;
        this.accessMode = accessMode;
        this.errorHandler = errorHandler;
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
        delegate.run( statement, parameters, newRoutingResponseHandler( runHandler ),
                newRoutingResponseHandler( pullAllHandler ) );
    }

    @Override
    public void runAndFlush( String statement, Map<String,Value> parameters, ResponseHandler runHandler,
            ResponseHandler pullAllHandler )
    {
        delegate.runAndFlush( statement, parameters, newRoutingResponseHandler( runHandler ),
                newRoutingResponseHandler( pullAllHandler ) );
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
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

    private RoutingResponseHandler newRoutingResponseHandler( ResponseHandler handler )
    {
        return new RoutingResponseHandler( handler, serverAddress(), accessMode, errorHandler );
    }
}
