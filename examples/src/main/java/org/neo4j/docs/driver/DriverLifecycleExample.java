package org.neo4j.docs.driver;

// tag::driver-lifecycle-import[]
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
// end::driver-lifecycle-import[]

// tag::driver-lifecycle[]
public class DriverLifecycleExample implements AutoCloseable
{
    private final Driver driver;

    public DriverLifecycleExample(String uri, String user, String password)
    {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws Exception
    {
        driver.close();
    }
}
// end::driver-lifecycle[]
