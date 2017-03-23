package org.neo4j.docs.driver;

// tag::result-retain-import[]

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;

import java.util.List;

import static org.neo4j.driver.v1.Values.parameters;
// end::result-retain-import[]

public class ResultRetainExample extends BaseApplication
{
    public ResultRetainExample(String uri, String user, String password)
    {
        super(uri, user, password);
    }

    // tag::result-retain[]
    public int addEmployees(String companyName)
    {
        try (Session session = driver.session())
        {
            int employees = 0;
            for (Record person : session.readTransaction(this::matchPersonNodes))
            {
                employees += session.writeTransaction((tx) -> {
                   tx.run("MATCH (emp:Person {name: $person_name}) " +
                           "MERGE (com:Company {name: $company_name}) " +
                           "MERGE (emp)-[:WORKS_FOR]->(com)",
                           parameters("person_name", person.get("name").asString(), "company_name", companyName));
                   return 1;
                });
            }
            return employees;
        }
    }

    private List<Record> matchPersonNodes(Transaction tx)
    {
        return tx.run("MATCH (a:Person) RETURN a.name AS name").list();
    }

    // end::result-retain[]

}
