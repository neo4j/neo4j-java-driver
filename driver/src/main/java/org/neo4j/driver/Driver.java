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
package org.neo4j.driver;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.StandardSession;
import org.neo4j.driver.internal.pool.StandardConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionPool;

/**
 * A driver to a Neo4j database.
 *
 * It provides methods to establish {@link Session sessions}, in which you can run statements.
 * <p>
 * An example:
 * <pre>
 * {@code
 *
 * // Create a driver with default configuration
 * Driver driver = GraphDatabase.driver( "neo4j://localhost:7687" );
 *
 * // Establish a session with a Neo4j instance
 * Session session = driver.session();
 * ** Do some work with the database... **
 *
 * // Release all the resources
 * driver.close();
 * }
 * </pre>
 * <p>
 * After a session is established, we could run statements
 * <p>
 * For example:
 * <pre>
 * {@code
 *
 * // Run a single statement
 * session.run( "CREATE (n {name:'Bob'})" );
 *
 * // Run multiple statements in a transaction
 * try( Transaction tx = session.newTransaction() )
 * {
 *     tx.run( "CREATE (n {name:'Alice'})" );
 *     tx.run( "CREATE (n {name:'Tina'})" );
 *     tx.success();
 * }
 *
 * // Retrieve results from a query
 * Result result = session.run( "MATCH (n) RETURN n.name" );
 * while( result.hasNext())
 * {
 *     Value record = result.next();
 *     System.out.println( record.get("n.name") );
 * }
 * }
 * </pre>
 * <p>
 *
 * A driver maintains a connection pool for each Neo4j instance. For resource efficiency reasons you are encouraged
 * to use the same driver instance across your application.
 */
public class Driver implements AutoCloseable
{
    private final ConnectionPool connections;
    private final URI url;

    public Driver( URI url, Config config )
    {
        this.url = url;
        this.connections = new StandardConnectionPool( config );
    }

    public Session session()
    {
        return new StandardSession( connections.acquire( url ) );
        // TODO a ConnectionPool per URL
        // ConnectionPool connections = new StandardConnectionPool( logging, url );
        // And to get a connection from the pool could be
        // connections.acquire();
    }

    public void close() throws Exception
    {
        connections.close();
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
