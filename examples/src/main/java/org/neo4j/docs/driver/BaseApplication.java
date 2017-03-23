package org.neo4j.docs.driver;


import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

public abstract class BaseApplication implements AutoCloseable
{
    protected final Driver driver;

    public BaseApplication(String uri, String user, String password)
    {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws Exception
    {
        driver.close();
    }
}
