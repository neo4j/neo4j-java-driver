package org.neo4j.docs.driver;

// tag::read-write-transaction-import[]

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;

import static org.neo4j.driver.v1.Values.parameters;
// end::read-write-transaction-import[]

public class ReadWriteTransactionExample extends BaseApplication
{
    public ReadWriteTransactionExample(String uri, String user, String password)
    {
        super(uri, user, password);
    }

    // tag::read-write-transaction[]
    public long addPerson(String name)
    {
        try (Session session = driver.session())
        {
            session.writeTransaction((tx) -> createPersonNode(tx, name));
            return session.readTransaction((tx) -> matchPersonNode(tx, name));
        }
    }

    private Void createPersonNode(Transaction tx, String name)
    {
        tx.run("CREATE (a:Person {name: $name})", parameters("name", name));
        return null;
    }

    private long matchPersonNode(Transaction tx, String name)
    {
        StatementResult result = tx.run("MATCH (a:Person {name: $name}) RETURN id(a)", parameters("name", name));
        return result.single().get(0).asLong();
    }
    // end::read-write-transaction[]

}
