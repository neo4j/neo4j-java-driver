package org.neo4j.driver.util;

import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;

/**
 * Create a static driver with default settings for tests.
 * This driver and its connection pool are reused by all tests using {@line TestSession} and {@link TestNeo4j}.
 * Any test that does not want to create its own driver could reuse sessions from this driver.
 * Finally this driver will not be destroyed but live in jvm all the time.
 */
public class Neo4jDriver
{
    private static Driver driver = GraphDatabase.driver( Neo4jRunner.DEFAULT_URL );

    public static Session session()
    {
        return driver.session();
    }
}
