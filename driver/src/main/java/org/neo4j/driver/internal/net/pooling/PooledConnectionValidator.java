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
package org.neo4j.driver.internal.net.pooling;

import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionValidator;
import org.neo4j.driver.internal.spi.PooledConnection;

class PooledConnectionValidator implements ConnectionValidator<PooledConnection>
{
    private final ConnectionPool pool;

    PooledConnectionValidator( ConnectionPool pool )
    {
        this.pool = pool;
    }

    @Override
    public boolean isReusable( PooledConnection pooledConnection )
    {
        // once the pooledConn has marked to have unrecoverable errors, there is no way to remove the error
        // and we should close the conn without bothering to reset the conn at all
        return pool.hasAddress( pooledConnection.boltServerAddress() ) &&
               !pooledConnection.hasUnrecoverableErrors() &&
               isConnected( pooledConnection );
    }

    @Override
    public boolean isConnected( PooledConnection connection )
    {
        try
        {
            // try to use this connection for RESET message
            // in case this session has an open result or transaction or something,
            // make sure it's reset to a nice state before we reuse it.
            connection.reset();
            connection.sync();
            return true;
        }
        catch ( Throwable e )
        {
            return false;
        }
    }
}
