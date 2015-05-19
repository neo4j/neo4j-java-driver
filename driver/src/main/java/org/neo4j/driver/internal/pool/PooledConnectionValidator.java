/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal.pool;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.internal.spi.StreamCollector;

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
