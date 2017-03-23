package org.neo4j.docs.driver;

// tag::config-trust-import[]
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
// end::config-trust-import[]

public class ConfigTrustExample implements AutoCloseable
{
    private final Driver driver;

    // tag::config-trust[]
    public ConfigTrustExample(String uri, String user, String password)
    {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password), 
                Config.build().withTrustStrategy(Config.TrustStrategy.trustSystemCertificates()).toConfig());
    }
    // end::config-trust[]

    @Override
    public void close() throws Exception
    {
        driver.close();
    }
}
