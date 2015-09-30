/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.util;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;

import org.neo4j.driver.Session;

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
                clearData();

                base.evaluate();
            }
        };
    }

    public void restartDatabase() throws IOException, InterruptedException
    {
        runner.stopServer();
        runner.startServer();
    }

    public String address()
    {
        return runner.DEFAULT_URL;
    }


    public boolean canControlServer()
    {
        return runner.canControlServer();
    }

    private void clearData()
    {
        // Note - this hangs for extended periods some times, because there are tests that leave sessions running.
        // Thus, we need to wait for open sessions and transactions to time out before this will go through.
        // This could be helped by an extension in the future.
        try ( Session session = Neo4jDriver.session() )
        {
            session.run( "MATCH (n) OPTIONAL MATCH (n)-[r]->() DELETE r,n" );
        }
    }
}
