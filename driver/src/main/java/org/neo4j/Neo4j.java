/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.StandardSession;
import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.internal.pool.StandardConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.Logging;

/**
 * The <strong>Neo4j</strong> class provides static methods to establish a
 * {@link org.neo4j.driver.Session} on a Neo4j database.
 * <p>
 * For example:
 * <p>
 * <pre>
 * {@code
 * Session session = Neo4j.session("neo4j://localhost:7687");
 *
 * // Run a single statement
 * session.run( "CREATE (n {name:'Bob'})" );
 *
 * // Run multiple statements in a transaction
 * try(Transaction tx = session.newTransaction())
 * {
 *     tx.run( "CREATE (n {name:'Alice'})" );
 *     tx.run( "CREATE (n {name:'Tina'})" );
 *     tx.success();
 * }
 *
 * // Retrieve results from a query
 * Result result = session.run("MATCH (n) RETURN n.name");
 * while(result.hasNext())
 * {
 *     Value record = result.next();
 *     System.out.println(record.get("n.name"));
 * }
 *
 * session.close();
 * }
 * </pre>
 */
public class Neo4j
{
    /**
     * Skinny log facade to allow users to inject their own logging in the future.
     */
    private static final Logging logging = new JULogging();

    /**
     * Live connections to databases
     */
    private static ConnectionPool connections = new StandardConnectionPool( logging );

    // Blocked constructor for this class as it only provides static methods.
    private Neo4j()
    {
    }

    /**
     * Establish a session with a Neo4j instance.
     *
     * @param sessionURL the URL to use to connect to neo4j and establish a session
     * @return a newly established session
     * @see #session(java.net.URI)
     */
    public static Session session( String sessionURL )
    {
        return session( URI.create( sessionURL ) );
    }

    /**
     * Establish a session with a Neo4j instance.
     * <p>
     * The session is established using a transport connector, which by default is the HTTP transport. You
     * specify the connector in the URL scheme. For the default transport, simply use {@code neo4j://<host>} and for
     * alternative connectors, use the {@code +} scheme-syntax. For instance, {@code neo4j+http://localhost} to
     * explicitly request the http transport.
     *
     * @param sessionURL the URL to use to connect to neo4j and establish a session
     * @return a newly established session
     */
    public static Session session( URI sessionURL )
    {
        return new StandardSession( connections.acquire( sessionURL ) );
    }

    /**
     * Helper function for creating a map of parameters, this can be used when you {@link
     * org.neo4j.driver.StatementRunner#run(String, java.util.Map) run} statements.
     * <p>
     * Allowed parameter types are java primitives and {@link java.lang.String} as well as
     * {@link java.util.Collection} and {@link java.util.Map} objects containing java
     * primitives and {@link java.lang.String} values.
     *
     * @param keysAndValues alternating sequence of keys and values
     * @return Map containing all parameters specified
     * @see org.neo4j.driver.StatementRunner#run(String, java.util.Map)
     */
    public static Map<String,Value> parameters( Object... keysAndValues )
    {
        if ( keysAndValues.length % 2 != 0 )
        {
            throw new ClientException( "Parameters function requires an even number " +
                                       "of arguments, " +
                                       "alternating key and value. Arguments were: " +
                                       Arrays.toString( keysAndValues ) + "." );
        }
        HashMap<String,Value> map = new HashMap<>( keysAndValues.length / 2 );
        for ( int i = 0; i < keysAndValues.length; i += 2 )
        {
            map.put( keysAndValues[i].toString(), Values.value( keysAndValues[i + 1] ) );
        }
        return map;
    }
}
