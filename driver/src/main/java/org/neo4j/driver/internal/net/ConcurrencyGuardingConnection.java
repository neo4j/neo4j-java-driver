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
package org.neo4j.driver.internal.net;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.summary.ServerInfo;

/**
 * This class ensures there can only ever be one thread using a connection at
 * the same time. Rather than doing this through synchronization, we do it by
 * throwing errors, because connections are not meant to be thread safe -
 * we simply want to inform the application it is using the session incorrectly.
 */
public class ConcurrencyGuardingConnection implements Connection
{
    private final Connection delegate;
    private final AtomicBoolean inUse = new AtomicBoolean( false );

    public ConcurrencyGuardingConnection( Connection delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public void init( String clientName, Map<String,Value> authToken )
    {
        try
        {
            markAsInUse();
            delegate.init(clientName, authToken);
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, ResponseHandler handler )
    {
        try
        {
            markAsInUse();
            delegate.run( statement, parameters, handler );
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void discardAll( ResponseHandler handler )
    {
        try
        {
            markAsInUse();
            delegate.discardAll( handler );
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void pullAll( ResponseHandler handler )
    {
        try
        {
            markAsInUse();
            delegate.pullAll( handler );
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void reset()
    {
        try
        {
            markAsInUse();
            delegate.reset();
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void ackFailure()
    {
        try
        {
            markAsInUse();
            delegate.ackFailure();
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void sync()
    {
        try
        {
            markAsInUse();
            delegate.sync();
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void flush()
    {
        try
        {
            markAsInUse();
            delegate.flush();
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void receiveOne()
    {
        try
        {
            markAsInUse();
            delegate.receiveOne();
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void close()
    {
        // It is fine to call close concurrently with this connection being used somewhere else.
        // This could happen when driver is closed while there still exist sessions that do some work.
        delegate.close();
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    @Override
    public void resetAsync()
    {
        delegate.resetAsync();
    }

    @Override
    public boolean isAckFailureMuted()
    {
        return delegate.isAckFailureMuted();
    }

    private void markAsAvailable()
    {
        inUse.set( false );
    }

    private void markAsInUse()
    {
        if(!inUse.compareAndSet( false, true ))
        {
            throw new ClientException( "You are using a session from multiple locations at the same time, " +
                                       "which is not supported. If you want to use multiple threads, you should ensure " +
                                       "that each session is used by only one thread at a time. One way to " +
                                       "do that is to give each thread its own dedicated session." );
        }
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
}
