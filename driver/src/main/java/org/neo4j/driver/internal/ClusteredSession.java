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
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClusterUnavailableException;
import org.neo4j.driver.v1.exceptions.ConnectionFailureException;

public class ClusteredSession extends NetworkSession
{
    private static final int RETRIES = 3;
    private final Supplier<Connection> connectionSupplier;

    ClusteredSession(Supplier<Connection> connectionSupplier, Logger logger )
    {
        super(connectionSupplier.get(), logger);
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public StatementResult run( Statement statement )
    {
        for ( int i = 0; i < RETRIES; i++ )
        {
            try
            {
                return super.run( statement );
            }
            catch ( ConnectionFailureException e )
            {
                //connection
                connection = connectionSupplier.get();
            }
        }

        throw new ClusterUnavailableException( "Not able to connect to any members of the cluster" );
    }
}
