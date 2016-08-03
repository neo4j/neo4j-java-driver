/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URL;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

public class TestNeo4j implements TestRule
{
    private final Neo4jSettings settings;
    private Neo4jRunner runner;

    public TestNeo4j()
    {
        this( Neo4jSettings.TEST_SETTINGS );
    }

    public TestNeo4j( Neo4jSettings settings )
    {
        this.settings = settings;
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
                runner.ensureRunning( settings );
                try ( Session session = driver().session() )
                {
                    clearDatabaseContents( session, description.toString() );
                }
                base.evaluate();
            }
        };
    }

    public Driver driver()
    {
        return runner.driver();
    }

    public void restart() throws Exception
    {
        runner.restartNeo4j();
    }

    public void forceRestart() throws Exception
    {
        runner.forceToRestart();
    }

    public void restart(Neo4jSettings neo4jSettings) throws Exception
    {
        runner.restartNeo4j( neo4jSettings );
    }

    public URL putTmpFile( String prefix, String suffix, String contents ) throws IOException
    {
        File tmpFile = File.createTempFile( prefix, suffix, null );
        tmpFile.deleteOnExit();
        try ( PrintWriter out = new PrintWriter( tmpFile ) )
        {
            out.println( contents );
        }
        return tmpFile.toURI().toURL();
    }

    public URI uri()
    {
        return Neo4jRunner.DEFAULT_URI;
    }

    public BoltServerAddress address()
    {
        return Neo4jRunner.DEFAULT_ADDRESS;
    }

    static void clearDatabaseContents( Session session, String reason )
    {
        Neo4jRunner.debug( "Clearing database contents for: %s", reason );

        // Note - this hangs for extended periods some times, because there are tests that leave sessions running.
        // Thus, we need to wait for open sessions and transactions to time out before this will go through.
        // This could be helped by an extension in the future.
        session.run( "MATCH (n) DETACH DELETE n" ).consume();
    }

    public void updateEncryptionKeyAndCert( File key, File cert ) throws Exception
    {
        FileTools.copyFile( key, Neo4jSettings.DEFAULT_TLS_KEY_FILE );
        FileTools.copyFile( cert, Neo4jSettings.DEFAULT_TLS_CERT_FILE );
        runner.forceToRestart(); // needs to force to restart as no configuration changed
    }

    public void ensureProcedures( String jarName ) throws IOException
    {
        File procedureJar = new File( Neo4jRunner.NEO4J_HOME, "plugins/" + jarName );
        if( !procedureJar.exists() )
        {
            FileTools.copyFile( new File( "src/test/resources", jarName ), procedureJar );
            runner.forceToRestart(); // needs to force to restart as no configuration changed
        }
    }
}
