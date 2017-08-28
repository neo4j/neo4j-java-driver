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
package org.neo4j.driver.internal.cluster;

import java.util.Map;
import java.util.Objects;

import org.neo4j.driver.internal.RoutingErrorHandler;
import org.neo4j.driver.internal.SessionResourcesHandler;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.summary.ServerInfo;

import static java.lang.String.format;

public class RoutingPooledConnection implements PooledConnection
{
    private final PooledConnection delegate;
    private final RoutingErrorHandler errorHandler;
    private final AccessMode accessMode;

    public RoutingPooledConnection( PooledConnection delegate, RoutingErrorHandler errorHandler, AccessMode accessMode )
    {
        this.delegate = delegate;
        this.errorHandler = errorHandler;
        this.accessMode = accessMode;
    }

    @Override
    public void init( String clientName, Map<String,Value> authToken )
    {
        try
        {
            delegate.init( clientName, authToken );
        }
        catch ( RuntimeException e )
        {
            throw handledException( e );
        }
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, ResponseHandler handler )
    {
        try
        {
            delegate.run( statement, parameters, handler );
        }
        catch ( RuntimeException e )
        {
            throw handledException( e );
        }
    }

    @Override
    public void discardAll( ResponseHandler handler )
    {
        try
        {
            delegate.discardAll( handler );
        }
        catch ( RuntimeException e )
        {
            throw handledException( e );
        }
    }

    @Override
    public void pullAll( ResponseHandler handler )
    {
        try
        {
            delegate.pullAll( handler );
        }
        catch ( RuntimeException e )
        {
            throw handledException( e );
        }
    }

    @Override
    public void reset()
    {
        try
        {
            delegate.reset();
        }
        catch ( RuntimeException e )
        {
            throw handledException( e );
        }
    }

    @Override
    public void resetAsync()
    {
        try
        {
            delegate.resetAsync();
        }
        catch ( RuntimeException e )
        {
            throw handledException( e );
        }
    }

    @Override
    public void ackFailure()
    {
        try
        {
            delegate.ackFailure();
        }
        catch ( RuntimeException e )
        {
            throw handledException( e );
        }
    }

    @Override
    public void sync()
    {
        try
        {
            delegate.sync();
        }
        catch ( RuntimeException e )
        {
            throw handledException( e );
        }
    }

    @Override
    public void flush()
    {
        try
        {
            delegate.flush();
        }
        catch ( RuntimeException e )
        {
            throw handledException( e );
        }
    }

    @Override
    public void receiveOne()
    {
        try
        {
            delegate.receiveOne();
        }
        catch ( RuntimeException e )
        {
            throw handledException( e );
        }
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    @Override
    public void setResourcesHandler( SessionResourcesHandler resourcesHandler )
    {
        delegate.setResourcesHandler( resourcesHandler );
    }

    @Override
    public boolean hasUnrecoverableErrors()
    {
        return delegate.hasUnrecoverableErrors();
    }

    @Override
    public boolean isAckFailureMuted()
    {
        return delegate.isAckFailureMuted();
    }

    @Override
    public ServerInfo server()
    {
        return delegate.server();
    }

    @Override
    public BoltServerAddress boltServerAddress()
    {
        return delegate.boltServerAddress();
    }

    @Override
    public long creationTimestamp()
    {
        return delegate.creationTimestamp();
    }

    @Override
    public long lastUsedTimestamp()
    {
        return delegate.lastUsedTimestamp();
    }

    @Override
    public void dispose()
    {
        delegate.dispose();
    }

    private RuntimeException handledException( RuntimeException e )
    {
        if ( e instanceof ServiceUnavailableException )
        {
            return handledServiceUnavailableException( ((ServiceUnavailableException) e) );
        }
        else if ( e instanceof ClientException )
        {
            return handledClientException( ((ClientException) e) );
        }
        else if ( e instanceof TransientException )
        {
            return handledTransientException( ((TransientException) e) );
        }
        else
        {
            return e;
        }
    }

    private RuntimeException handledServiceUnavailableException( ServiceUnavailableException e )
    {
        BoltServerAddress address = boltServerAddress();
        errorHandler.onConnectionFailure( address );
        return new SessionExpiredException( format( "Server at %s is no longer available", address ), e );
    }

    private RuntimeException handledTransientException( TransientException e )
    {
        String errorCode = e.code();
        if ( Objects.equals( errorCode, "Neo.TransientError.General.DatabaseUnavailable" ) )
        {
            BoltServerAddress address = boltServerAddress();
            errorHandler.onConnectionFailure( address );
        }
        return e;
    }

    private RuntimeException handledClientException( ClientException e )
    {
        if ( isFailureToWrite( e ) )
        {
            // The server is unaware of the session mode, so we have to implement this logic in the driver.
            // In the future, we might be able to move this logic to the server.
            switch ( accessMode )
            {
            case READ:
                return new ClientException( "Write queries cannot be performed in READ access mode." );
            case WRITE:
                BoltServerAddress address = boltServerAddress();
                errorHandler.onWriteFailure( address );
                return new SessionExpiredException( format( "Server at %s no longer accepts writes", address ) );
            default:
                throw new IllegalArgumentException( accessMode + " not supported." );
            }
        }
        return e;
    }

    private static boolean isFailureToWrite( ClientException e )
    {
        String errorCode = e.code();
        return Objects.equals( errorCode, "Neo.ClientError.Cluster.NotALeader" ) ||
               Objects.equals( errorCode, "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase" );
    }
}
