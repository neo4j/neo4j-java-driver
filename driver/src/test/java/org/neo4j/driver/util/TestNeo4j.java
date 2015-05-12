package org.neo4j.driver.util;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;

public class TestNeo4j implements TestRule
{
    private Neo4jRunner runner;

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                runner = Neo4jRunner.getOrCreateGlobalServer();
                runner.clearData();

                base.evaluate();
            }
        };
    }

    public String address()
    {
        return runner.address();
    }

    public void restartDatabase() throws IOException, InterruptedException
    {
        runner.stopServer();
        runner.startServer();
    }

    public boolean canControlServer()
    {
        return runner.canControlServer();
    }
}
