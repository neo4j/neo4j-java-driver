package org.neo4j.docs.driver;

// tag::autocommit-transaction-import[]

import org.neo4j.driver.v1.Session;

import static org.neo4j.driver.v1.Values.parameters;
// end::autocommit-transaction-import[]

public class AutocommitTransactionExample extends BaseApplication
{
    public AutocommitTransactionExample(String uri, String user, String password)
    {
        super(uri, user, password);
    }

    // tag::autocommit-transaction[]
    public void addPerson(String name)
    {
        try (Session session = driver.session())
        {
            session.run("CREATE (a:Person {name: $name})", parameters("name", name));
        }
    }
    // end::autocommit-transaction[]

}
