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
package org.neo4j.driver.v1.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Result;
import org.neo4j.driver.v1.Session;

public class TestNeo4j implements TestRule
{
    private final Neo4jResetMode resetMode;
    private final Neo4jSettings initialSettings;

    private Neo4jRunner runner;

    public TestNeo4j()
    {
        this( Neo4jSettings.DEFAULT, Neo4jResetMode.CLEAR_DATABASE_CONTENTS );
    }

    public TestNeo4j( Neo4jResetMode resetMode )
    {
        this( Neo4jSettings.DEFAULT, resetMode );
    }

    public TestNeo4j( Neo4jSettings initialSettings )
    {
        this( initialSettings, Neo4jResetMode.CLEAR_DATABASE_CONTENTS );
    }

    public TestNeo4j( Neo4jSettings initialSettings, Neo4jResetMode resetMode )
    {
        this.initialSettings = initialSettings;
        this.resetMode = resetMode;
    }

    @Override
    public Statement apply( final Statement base, final Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
            runner = Neo4jRunner.getOrCreateGlobalRunner();
            switch ( resetMode )
            {
                case CLEAR_DATABASE_CONTENTS:
                    if ( !runner.startServerOnEmptyDatabaseUnlessRunning( initialSettings ) )
                    {
                        try ( Session session = driver().session() )
                        {
                            clearDatabaseContents( session, description.toString() );
                        }
                    }
                    break;

                case CLEAR_DATABASE_FILES:
                    restartServerOnEmptyDatabase( initialSettings );
                    break;
            }

            base.evaluate();
            }
        };
    }

    public Driver driver()
    {
        return runner.driver();
    }

    public void restartServerOnEmptyDatabase() throws Exception
    {
        runner.restartServerOnEmptyDatabase();
    }

    public void restartServerOnEmptyDatabase( Neo4jSettings settingsUpdate ) throws Exception
    {
        runner.restartServerOnEmptyDatabase( settingsUpdate );
    }

    public URL putTmpFile( String prefix, String suffix, String contents ) throws IOException
    {
        File tmpFile = File.createTempFile( prefix, suffix, null );
        tmpFile.deleteOnExit();
        try ( PrintWriter out = new PrintWriter( tmpFile ) )
        {
            out.println( contents);
        }
        return tmpFile.toURI().toURL();
    }

    public String address()
    {
        return Neo4jRunner.DEFAULT_URL;
    }

    public boolean canControlServer()
    {
        return runner.canControlServer();
    }

    @SuppressWarnings("StatementWithEmptyBody")
    static void clearDatabaseContents( Session session, String reason )
    {
        Neo4jRunner.debug( "Clearing database contents for: %s", reason );

        // Note - this hangs for extended periods some times, because there are tests that leave sessions running.
        // Thus, we need to wait for open sessions and transactions to time out before this will go through.
        // This could be helped by an extension in the future.
        Result result = session.run( "MATCH (n) DETACH DELETE n RETURN count(*)" );
        while ( result.next() )
        {
            // consume
        }
    }
}
