package org.neo4j.docs.driver;

// tag::service-unavailable-import[]

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.util.concurrent.TimeUnit.SECONDS;
// end::service-unavailable-import[]

public class ServiceUnavailableExample implements AutoCloseable
{
    protected final Driver driver;

    public ServiceUnavailableExample(String uri, String user, String password)
    {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password), Config.build().withMaxTransactionRetryTime(3, SECONDS).toConfig());
    }

    @Override
    public void close() throws Exception
    {
        driver.close();
    }

    // tag::service-unavailable[]
    public boolean addItem()
    {
        try (Session session = driver.session())
        {
            return session.writeTransaction((tx) ->
            {
                tx.run("CREATE (a:Item)");
                return true;
            });
        }
        catch (ServiceUnavailableException ex)
        {
            return false;
        }
    }
    // end::service-unavailable[]

}
