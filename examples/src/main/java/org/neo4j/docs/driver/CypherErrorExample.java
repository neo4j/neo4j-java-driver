package org.neo4j.docs.driver;

// tag::cypher-error-import[]

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;

import static org.neo4j.driver.v1.Values.parameters;
// end::cypher-error-import[]

public class CypherErrorExample extends BaseApplication
{
    public CypherErrorExample(String uri, String user, String password)
    {
        super(uri, user, password);
    }

    // tag::cypher-error[]
    public int getEmployeeNumber(String name)
    {
        try (Session session = driver.session())
        {
            return session.readTransaction((tx) -> selectEmployee(tx, name));
        }
    }

    private int selectEmployee(Transaction tx, String name)
    {
        try
        {
            StatementResult result = tx.run("SELECT * FROM Employees WHERE name = $name", parameters("name", name));
            return result.single().get("employee_number").asInt();
        }
        catch (ClientException ex)
        {
            System.err.println(ex.getMessage());
            return -1;
        }
    }
    // end::cypher-error[]

}
