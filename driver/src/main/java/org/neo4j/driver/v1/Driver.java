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
package org.neo4j.driver.v1;

import java.net.URI;

import org.neo4j.driver.v1.internal.StandardSession;
import org.neo4j.driver.v1.internal.pool.StandardConnectionPool;
import org.neo4j.driver.v1.internal.spi.ConnectionPool;

/**
 * A Neo4j database driver, through which you can create {@link Session sessions} to run statements against the database.
 * <p>
 * An example:
 * <pre class="doctest:DriverDocIT#exampleUsage">
 * {@code
 * // Create a driver with default configuration
 * Driver driver = GraphDatabase.driver( "bolt://localhost:7687" );
 *
 * // Establish a session
 * Session session = driver.session();
 *
 * // Running a simple statement can be done like this
 * session.run( "CREATE (n {name:'Bob'})" );
 *
 * // Or, run multiple statements together in an atomic transaction:
 * try( Transaction tx = session.beginTransaction() )
 * {
 *     tx.run( "CREATE (n {name:'Alice'})" );
 *     tx.run( "CREATE (n {name:'Tina'})" );
 *     tx.success();
 * }
 *
 * // Retrieve results
 * Result result = session.run( "MATCH (n) RETURN n.name" );
 * List<String> names = new LinkedList<>();
 * while( result.next() )
 * {
 *     names.add( result.get("n.name").javaString() );
 * }
 *
 * // Sessions are pooled, to avoid the overhead of creating new connections - this means
 * // it is very important to close your session when you are done with it, otherwise you will
 * // run out of sessions.
 * session.close();
 *
 * // And, to clean up resources, always close the driver when your application is done
 * driver.close();
 * }
 * </pre>
 * <p>
 *
 * A driver maintains a connection pool for each Neo4j instance. For resource efficiency reasons you are encouraged
 * to use the same driver instance across your application. You can control the connection pooling behavior when you
 * create the driver using the {@link Config} you pass into {@link GraphDatabase#driver(URI, Config)}.
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

    /**
     * Establish a session
     * @return a session that could be used to run {@link Session#run(String) a statement} or
     * {@link Session#beginTransaction() a transaction }.
     */
    public Session session()
    {
        return new StandardSession( connections.acquire( url ) );
        // TODO a ConnectionPool per URL
        // ConnectionPool connections = new StandardConnectionPool( logging, url );
        // And to get a connection from the pool could be
        // connections.acquire();
    }

    /**
     * Close all the resources assigned to this driver
     * @throws Exception any error that might happen when releasing all resources
     */
    public void close() throws Exception
    {
        connections.close();
    }
}
