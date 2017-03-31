package org.neo4j.docs.driver;

// tag::basic-auth-import[]
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.StatementResult;
// end::basic-auth-import[]

public class BasicAuthExample implements AutoCloseable
{
    private final Driver driver;

    // tag::basic-auth[]
    public BasicAuthExample(String uri, String user, String password)
    {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }
    // end::basic-auth[]

    @Override
    public void close() throws Exception
    {
        driver.close();
    }

    public boolean canConnect()
    {
        StatementResult result = driver.session().run("RETURN 1");
        return result.single().get(0).asInt() == 1;
    }
}
