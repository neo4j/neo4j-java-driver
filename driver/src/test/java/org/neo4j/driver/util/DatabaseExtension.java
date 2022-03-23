/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URL;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.types.TypeSystem;

import static java.lang.Integer.parseInt;
import static org.neo4j.driver.util.Neo4jRunner.DEFAULT_AUTH_TOKEN;
import static org.neo4j.driver.util.Neo4jRunner.HOME_DIR;
import static org.neo4j.driver.util.Neo4jRunner.debug;
import static org.neo4j.driver.util.Neo4jRunner.getOrCreateGlobalRunner;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_TLS_CERT_PATH;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_TLS_KEY_PATH;

public class DatabaseExtension implements BeforeEachCallback, AfterAllCallback
{
    static final String TEST_RESOURCE_FOLDER_PATH = "src/test/resources";

    private final Neo4jSettings settings;
    private Neo4jRunner runner;

    public DatabaseExtension()
    {
        this( Neo4jSettings.TEST_SETTINGS );
    }

    public DatabaseExtension( Neo4jSettings settings )
    {
        this.settings = settings;
    }

    @Override
    public void beforeEach( ExtensionContext context ) throws Exception
    {
        runner = getOrCreateGlobalRunner();
        runner.ensureRunning( settings );
        TestUtil.cleanDb( driver() );
    }

    @Override
    public void afterAll( ExtensionContext context )
    {
        if ( runner != null )
        {
            runner.stopNeo4j();
        }
    }

    public Driver driver()
    {
        return runner.driver();
    }

    public TypeSystem typeSystem()
    {
        return driver().defaultTypeSystem();
    }

    public void forceRestartDb()
    {
        runner.forceToRestart();
    }

    public void restartDb( Neo4jSettings neo4jSettings )
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
        return runner.boltUri();
    }

    public int httpPort()
    {
        return runner.httpPort();
    }

    public int boltPort()
    {
        return runner.boltPort();
    }

    public AuthToken authToken()
    {
        return DEFAULT_AUTH_TOKEN;
    }

    public BoltServerAddress address()
    {
        return runner.boltAddress();
    }

    public void updateEncryptionKeyAndCert( File key, File cert ) throws Exception
    {
        FileTools.copyFile( key, tlsKeyFile() );
        FileTools.copyFile( cert, tlsCertFile() );
        debug( "Updated neo4j key and certificate file." );
        runner.forceToRestart(); // needs to force to restart as no configuration changed
    }

    public File tlsCertFile()
    {
        return new File( HOME_DIR, DEFAULT_TLS_CERT_PATH );
    }

    public File tlsKeyFile()
    {
        return new File( HOME_DIR, DEFAULT_TLS_KEY_PATH );
    }

    public void startDb()
    {
        runner.startNeo4j();
    }

    public void stopDb()
    {
        runner.stopNeo4j();
    }

    public boolean isNeo4j43OrEarlier()
    {
        try ( Session session = driver().session() )
        {
            String neo4jVersion = session.readTransaction( tx -> tx.run( "CALL dbms.components() YIELD versions " +
                                                                            "RETURN versions[0] AS version" ).single().get( "version" ).asString() );
            String[] versions = neo4jVersion.split( "\\." );
            int major = parseInt( versions[0] );
            int minor = parseInt( versions[1] );
            return  major <= 4 && minor <= 3;
        }
    }

    public void dumpLogs()
    {
        runner.dumpDebugLog();
    }
}
