package org.neo4j.docs.driver;

// tag::custom-auth-import[]
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import java.util.Map;
// end::custom-auth-import[]

public class CustomAuthExample implements AutoCloseable
{
    private final Driver driver;

    // tag::custom-auth[]
    public CustomAuthExample(String uri, String principal, String credentials, String realm, String scheme, Map<String, Object> parameters)
    {
        driver = GraphDatabase.driver(uri, AuthTokens.custom(principal, credentials, realm, scheme, parameters));
    }
    // end::custom-auth[]

    @Override
    public void close() throws Exception
    {
        driver.close();
    }
}
