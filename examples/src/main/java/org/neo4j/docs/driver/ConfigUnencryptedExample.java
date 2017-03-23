package org.neo4j.docs.driver;

// tag::config-unencrypted-import[]
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
// end::config-unencrypted-import[]

public class ConfigUnencryptedExample implements AutoCloseable
{
    private final Driver driver;

    // tag::config-unencrypted[]
    public ConfigUnencryptedExample(String uri, String user, String password)
    {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password), 
                Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
    }
    // end::config-unencrypted[]

    @Override
    public void close() throws Exception
    {
        driver.close();
    }
}
