package org.neo4j.docs.driver;

// tag::session-import[]

import org.neo4j.driver.v1.Session;
// end::session-import[]

public class SessionExample extends BaseApplication
{
    public SessionExample(String uri, String user, String password)
    {
        super(uri, user, password);
    }

    // tag::session[]
    public void doWork()
    {
        try (Session session = driver.session())
        {
            // TODO: something with the Session
        }
    }
    // end::session[]

}
