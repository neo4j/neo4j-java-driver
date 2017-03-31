package org.neo4j.docs.driver;

// tag::result-consume-import[]

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;

import java.util.ArrayList;
import java.util.List;
// end::result-consume-import[]

public class ResultConsumeExample extends BaseApplication
{
    public ResultConsumeExample(String uri, String user, String password)
    {
        super(uri, user, password);
    }

    // tag::result-consume[]
    public List<String> getPeople()
    {
        try (Session session = driver.session())
        {
            return session.readTransaction(this::matchPersonNodes);
        }
    }

    private List<String> matchPersonNodes(Transaction tx)
    {
        List<String> names = new ArrayList<>();
        StatementResult result = tx.run("MATCH (a:Person) RETURN a.name ORDER BY a.name");
        while (result.hasNext())
        {
            names.add(result.next().get(0).asString());
        }
        return names;
    }
    // end::result-consume[]

}
