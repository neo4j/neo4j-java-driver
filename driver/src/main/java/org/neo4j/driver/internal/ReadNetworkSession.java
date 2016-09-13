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


import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ConnectionFailureException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

public class ReadNetworkSession extends NetworkSession
{
    private final Supplier<Connection> connectionSupplier;
    private final Consumer<Connection> failed;
    private final ClusterSettings clusterSettings;

    ReadNetworkSession(Supplier<Connection> connectionSupplier, Consumer<Connection> failed,
            ClusterSettings clusterSettings, Logger logger )
    {
        super(connectionSupplier.get(), logger);
        this.connectionSupplier = connectionSupplier;
        this.clusterSettings = clusterSettings;
        this.failed = failed;
    }

    @Override
    public StatementResult run( Statement statement )
    {
        for ( int i = 0; i < clusterSettings.readRetry(); i++ )
        {
            try
            {
                return super.run( statement );
            }
            catch ( ConnectionFailureException e )
            {
                failed.accept(connection);
                connection = connectionSupplier.get();
            }
        }

        throw new ServiceUnavailableException( "Not able to connect to any members of the cluster" );
    }
}
