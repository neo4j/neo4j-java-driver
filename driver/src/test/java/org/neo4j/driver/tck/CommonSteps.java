/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.driver.tck;


import cucumber.api.java.After;
import cucumber.api.java.Before;

import java.io.IOException;

import org.neo4j.driver.Result;
import org.neo4j.driver.ResultSummary;
import org.neo4j.driver.Session;
import org.neo4j.driver.util.Neo4jDriver;
import org.neo4j.driver.util.Neo4jRunner;

public class CommonSteps
{
    private Neo4jRunner neo4j;

    static class Context
    {
        Session session;
        Result result;
        ResultSummary summary;
    }

    public static Context ctx = new Context();

    @Before
    public void setup() throws IOException, InterruptedException
    {
        neo4j = Neo4jRunner.getOrCreateGlobalServer();
        try ( Session session = Neo4jDriver.session() )
        {
            session.run( "MATCH (n) OPTIONAL MATCH (n)-[r]->() DELETE r,n" );
        }
        ctx = new Context();
        ctx.session = Neo4jDriver.session();
    }

    @After
    public void cleanup()
    {
        if(ctx.session != null)
        {
            ctx.session.close();
        }
    }
}
