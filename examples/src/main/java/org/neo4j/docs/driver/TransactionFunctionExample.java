package org.neo4j.docs.driver;

// tag::transaction-function-import[]

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;

import static org.neo4j.driver.v1.Values.parameters;
// end::transaction-function-import[]

public class TransactionFunctionExample extends BaseApplication
{
    public TransactionFunctionExample(String uri, String user, String password)
    {
        super(uri, user, password);
    }

    // tag::transaction-function[]
    public void addPerson(String name)
    {
        try (Session session = driver.session())
        {
            session.writeTransaction((tx) -> createPersonNode(tx, name));
        }
    }
    private int createPersonNode(Transaction tx, String name)
    {
        tx.run("CREATE (a:Person {name: $name})", parameters("name", name));
        return 1;
    }
    // end::transaction-function[]

}
