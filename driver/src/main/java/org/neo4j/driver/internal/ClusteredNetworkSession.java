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
package org.neo4j.driver.internal;


import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ConnectionFailureException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

public class ClusteredNetworkSession extends NetworkSession
{
    private final Consumer<BoltServerAddress> onFailedConnection;
    private final AtomicBoolean isOpen = new AtomicBoolean( true );

    ClusteredNetworkSession( Connection connection, ClusterSettings clusterSettings, Consumer<BoltServerAddress> onFailedConnection, Logger logger )
    {
        super( connection, logger );
        this.onFailedConnection = onFailedConnection;
    }

    @Override
    public StatementResult run( Statement statement )
    {
        try
        {
            return new ClusteredStatementResult( super.run( statement ), connection.address(),
                    new Consumer<BoltServerAddress>()
                    {
                        @Override
                        public void accept( BoltServerAddress address )
                        {
                            onFailedConnection.accept( address );
                        }
                    } );
        }//TODO we need to catch exceptions due to leader switches etc here
        catch ( ConnectionFailureException e )
        {
            onFailedConnection.accept( connection.address() );
            throw new SessionExpiredException( "Failed to perform write load to server", e );
        }
    }

    @Override
    public void close()
    {
        try
        {
            super.close();
        }
        catch ( ConnectionFailureException e )
        {
            BoltServerAddress address = connection.address();
            onFailedConnection.accept( address );
            throw new SessionExpiredException( String.format( "Server at %s is no longer available", address.toString()), e);
        }
    }
}
