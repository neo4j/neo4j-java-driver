package org.neo4j.docs.driver;

// tag::config-max-retry-time-import[]

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import static java.util.concurrent.TimeUnit.SECONDS;
// end::config-max-retry-time-import[]

public class ConfigMaxRetryTimeExample implements AutoCloseable
{
    private final Driver driver;

    // tag::config-max-retry-time[]
    public ConfigMaxRetryTimeExample(String uri, String user, String password)
    {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password), 
                Config.build().withMaxTransactionRetryTime(15, SECONDS).toConfig());
    }
    // end::config-max-retry-time[]

    @Override
    public void close() throws Exception
    {
        driver.close();
    }
}
