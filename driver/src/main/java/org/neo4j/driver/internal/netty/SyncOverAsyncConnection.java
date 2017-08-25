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
package org.neo4j.driver.internal.netty;

import io.netty.channel.ChannelPromise;

import java.util.Map;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.summary.ServerInfo;

public class SyncOverAsyncConnection implements Connection
{
    private final AsyncConnection asyncConnection;

    public SyncOverAsyncConnection( AsyncConnection asyncConnection )
    {
        this.asyncConnection = asyncConnection;
    }

    @Override
    public void init( String clientName, Map<String,Value> authToken )
    {
        // async connection is initialized by default
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, final Collector collector )
    {
        asyncConnection.run( statement, parameters, new ResponseHandler()
        {
            @Override
            public void onSuccess( Map<String,Value> metadata )
            {
                collectFields( collector, metadata.get( "fields" ) );
                collector.doneSuccess();
            }

            @Override
            public void onFailure( Throwable error )
            {
                collector.doneFailure( new ClientException( "", error ) );
            }

            @Override
            public void onRecord( Value[] fields )
            {
                throw new UnsupportedOperationException();
            }
        } );
    }

    private void collectFields( Collector collector, Value fieldValue )
    {
        if ( fieldValue != null )
        {
            if ( !fieldValue.isEmpty() )
            {
                String[] fields = new String[fieldValue.size()];
                int idx = 0;
                for ( Value value : fieldValue.values() )
                {
                    fields[idx++] = value.asString();
                }
                collector.keys( fields );
            }
        }
    }

    @Override
    public void discardAll( Collector collector )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void pullAll( final Collector collector )
    {
        final ChannelPromise promise = asyncConnection.newPromise();
        asyncConnection.pullAll( new ResponseHandler()
        {
            @Override
            public void onSuccess( Map<String,Value> metadata )
            {
                promise.setSuccess();
            }

            @Override
            public void onFailure( Throwable error )
            {
                promise.setFailure( error );
            }

            @Override
            public void onRecord( Value[] fields )
            {
                collector.record( fields );
            }
        } );

        promise.awaitUninterruptibly();
        collector.done();

        if ( !promise.isSuccess() )
        {
            throw new RuntimeException( promise.cause() );
        }
    }

    @Override
    public void reset()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ackFailure()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync()
    {
        asyncConnection.flush();
    }

    @Override
    public void flush()
    {
        asyncConnection.flush();
    }

    @Override
    public void receiveOne()
    {
    }

    @Override
    public void close()
    {
        asyncConnection.close();
    }

    @Override
    public boolean isOpen()
    {
        return asyncConnection.isOpen();
    }

    @Override
    public void resetAsync()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAckFailureMuted()
    {
        return false;
    }

    @Override
    public ServerInfo server()
    {
        return null;
    }

    @Override
    public BoltServerAddress boltServerAddress()
    {
        return null;
    }
}
