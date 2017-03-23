package org.neo4j.docs.driver;

// tag::config-connection-timeout-import[]

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import static java.util.concurrent.TimeUnit.SECONDS;
// end::config-connection-timeout-import[]

public class ConfigConnectionTimeoutExample implements AutoCloseable
{
    private final Driver driver;

    // tag::config-connection-timeout[]
    public ConfigConnectionTimeoutExample(String uri, String user, String password)
    {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password), 
                Config.build().withConnectionTimeout(15, SECONDS).toConfig());
    }
    // end::config-connection-timeout[]

    @Override
    public void close() throws Exception
    {
        driver.close();
    }
}
