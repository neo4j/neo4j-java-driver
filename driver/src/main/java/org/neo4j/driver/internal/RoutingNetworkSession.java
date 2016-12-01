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
package org.neo4j.driver.internal;


import java.util.Map;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.types.TypeSystem;

import static java.lang.String.format;
import static org.neo4j.driver.v1.Values.value;

/**
 * A session that safely handles routing errors.
 */
public class RoutingNetworkSession implements Session
{
    protected final Session delegate;
    private final BoltServerAddress address;
    private final AccessMode mode;
    private final RoutingErrorHandler onError;

    RoutingNetworkSession( Session delegate, AccessMode mode, BoltServerAddress address,
            RoutingErrorHandler onError )
    {
        this.delegate = delegate;
        this.mode = mode;
        this.address = address;
        this.onError = onError;
    }

    @Override
    public StatementResult run( String statementText )
    {
        return run( statementText, Values.EmptyMap );
    }

    @Override
    public StatementResult run( String statementText, Map<String,Object> statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters );
        return run( statementText, params );
    }

    @Override
    public StatementResult run( String statementTemplate, Record statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters.asMap() );
        return run( statementTemplate, params );
    }

    @Override
    public StatementResult run( String statementText, Value statementParameters )
    {
        return run( new Statement( statementText, statementParameters ) );
    }

    @Override
    public StatementResult run( Statement statement )
    {
        try
        {
            return new RoutingStatementResult( delegate.run( statement ), mode, address, onError );
        }
        catch ( ServiceUnavailableException e )
        {
            throw sessionExpired( e, onError, address );
        }
        catch ( ClientException e )
        {
            throw filterFailureToWrite( e, mode, onError, address );
        }
    }

    @Override
    public TypeSystem typeSystem()
    {
        return delegate.typeSystem();
    }

    @Override
    public Transaction beginTransaction()
    {
        return new RoutingTransaction( delegate.beginTransaction(), mode, address, onError);
    }

    @Override
    public Transaction beginTransaction( String bookmark )
    {
        return new RoutingTransaction( delegate.beginTransaction(bookmark), mode, address, onError);
    }

    @Override
    public String lastBookmark()
    {
        return delegate.lastBookmark();
    }

    @Override
    public void reset()
    {
        delegate.reset();
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    @Override
    public void close()
    {
        try
        {
            delegate.close();
        }
        catch ( ServiceUnavailableException e )
        {
            throw sessionExpired(e, onError, address);
        }
        catch ( ClientException e )
        {
            throw filterFailureToWrite( e, mode, onError, address );
        }
    }

    public BoltServerAddress address()
    {
        return address;
    }

    static Neo4jException filterFailureToWrite( ClientException e, AccessMode mode, RoutingErrorHandler onError,
            BoltServerAddress address )
    {
        if ( isFailedToWrite( e ) )
        {
            // The server is unaware of the session mode, so we have to implement this logic in the driver.
            // In the future, we might be able to move this logic to the server.
            switch ( mode )
            {
                case READ:
                    return new ClientException( "Write queries cannot be performed in READ access mode." );
                case WRITE:
                    onError.onWriteFailure( address );
                    return new SessionExpiredException( format( "Server at %s no longer accepts writes", address ) );
                default:
                    throw new IllegalArgumentException( mode + " not supported." );
            }
        }
        else
        {
            return e;
        }
    }

    static SessionExpiredException sessionExpired( ServiceUnavailableException e, RoutingErrorHandler onError,
                                                   BoltServerAddress address )
    {
        onError.onConnectionFailure( address );
        return new SessionExpiredException( format( "Server at %s is no longer available", address.toString() ), e );
    }

    private static boolean isFailedToWrite( ClientException e )
    {
        return e.code().equals( "Neo.ClientError.Cluster.NotALeader" ) ||
                e.code().equals( "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase" );
    }
}
