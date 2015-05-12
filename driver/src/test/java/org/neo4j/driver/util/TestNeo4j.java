/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
