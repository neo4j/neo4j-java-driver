/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.internal.pool;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.internal.spi.StreamCollector;

/**
 * Validates connections - determining if they are ok to keep in the pool, or if they should be disposed of.
 */
public class PooledConnectionValidator implements ValidationStrategy<PooledConnection>
{
    private static final Map<String,Value> NO_PARAMETERS = new HashMap<>();

    /**
     * Connections that have been idle longer than this threshold will have a ping test performed on them.
     */
    private final long minIdleBeforeConnectionTest;

    public PooledConnectionValidator( long minIdleBeforeConnectionTest )
    {
        this.minIdleBeforeConnectionTest = minIdleBeforeConnectionTest;
    }

    @Override
    public boolean isValid( PooledConnection conn, long idleTime )
    {
        if ( conn.hasUnrecoverableErrors() )
        {
            return false;
        }

        return idleTime <= minIdleBeforeConnectionTest || ping( conn );
    }

    private boolean ping( PooledConnection conn )
    {
        try
        {
            conn.run( "RETURN 1 // JavaDriver poll to test connection", NO_PARAMETERS, StreamCollector.NO_OP );
            conn.pullAll( StreamCollector.NO_OP );
            conn.sync();
            return true;
        } catch( Throwable e )
        {
            return false;
        }
    }
}
