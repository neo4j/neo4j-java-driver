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
package org.neo4j.driver.internal.net.pooling;

import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.v1.util.Function;

class PooledConnectionValidator implements Function<PooledConnection,Boolean>
{
    private final ConnectionPool pool;

    PooledConnectionValidator( ConnectionPool pool )
    {
        this.pool = pool;
    }

    @Override
    public Boolean apply( PooledConnection pooledConnection )
    {
        // once the pooledConn has marked to have unrecoverable errors, there is no way to remove the error
        // and we should close the conn without bothering to reset the conn at all
        return pool.hasAddress( pooledConnection.boltServerAddress() ) &&
               !pooledConnection.hasUnrecoverableErrors() &&
               reset( pooledConnection );
    }

    /**
     * In case this session has an open result or transaction or something,
     * make sure it's reset to a nice state before we reuse it.
     *
     * @param conn the PooledConnection
     * @return true if the connection is reset successfully without any error, otherwise false.
     */
    private static boolean reset( PooledConnection conn )
    {
        try
        {
            conn.reset();
            conn.sync();
            return true;
        }
        catch ( Throwable e )
        {
            return false;
        }
    }
}
