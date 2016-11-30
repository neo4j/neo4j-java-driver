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
package org.neo4j.driver.internal.net.pooling;

import org.neo4j.driver.internal.exceptions.InvalidOperationException;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.v1.util.Function;

/**
 * The responsibility of the {@link PooledConnectionReleaseManager} is to release valid connections
 * back to the connections queue.
 */
public class PooledConnectionReleaseManager implements PooledConnectionReleaser
{
    private final BlockingPooledConnectionQueue connections;
    private final Function<PooledConnection, Boolean> connectionValidator;

    protected PooledConnectionReleaseManager( BlockingPooledConnectionQueue conns,
            Function<PooledConnection,Boolean> validator )
    {
        this.connections = conns;
        this.connectionValidator = validator;
    }

    @Override
    public void accept( PooledConnection pooledConnection ) throws InvalidOperationException
    {
        if ( connectionValidator.apply( pooledConnection ) )
        {
            connections.offer( pooledConnection );
        }
        else
        {
            pooledConnection.dispose();
        }
    }
}
