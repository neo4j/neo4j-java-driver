/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Driver;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.types.TypeSystem;

import static org.neo4j.driver.util.DockerBasedNeo4jRunner.getOrCreateGlobalRunner;
import static org.neo4j.driver.util.Neo4jRunner.DEFAULT_AUTH_TOKEN;
import static org.neo4j.driver.util.Neo4jRunner.debug;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_TLS_CERT_FILE;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_TLS_KEY_FILE;

public class DatabaseExtension implements BeforeEachCallback
{
    private final Neo4jSettings settings;
    private Neo4jRunner runner;
    private static final String TEST_RESOURCE_FOLDER_PATH = "src/test/resources";

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

    public Driver driver()
    {
        return runner.driver();
    }

    public TypeSystem typeSystem()
    {
        return driver().defaultTypeSystem();
    }

    public void restartDb()
    {
        runner.restartNeo4j();
    }

    public void restartDb( Neo4jSettings neo4jSettings )
    {
        runner.restartNeo4j( neo4jSettings );
    }

    public String putTmpCsvFile( String prefix, String suffix, String contents ) throws IOException
    {
        return putTmpCsvFile( prefix, suffix, Collections.singletonList( contents ) );
    }

    public String putTmpCsvFile( String prefix, String suffix, Iterable<String> lines ) throws IOException
    {
        File tempFile = File.createTempFile( prefix, suffix, runner.importDirectory() );
        tempFile.deleteOnExit();
        Files.write( Paths.get( tempFile.getAbsolutePath() ), lines );
        return "file:/" + tempFile.getName();
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
        runner.restartNeo4j(); // needs to force to restart as no configuration changed
    }

    public File tlsCertFile()
    {
        return new File( runner.certificatesDirectory(), DEFAULT_TLS_CERT_FILE );
    }

    public File tlsKeyFile()
    {
        return new File( runner.certificatesDirectory(), DEFAULT_TLS_KEY_FILE );
    }

    public void startDb()
    {
        runner.startNeo4j();
    }

    public void stopDb()
    {
        runner.stopNeo4j();
    }

    public ServerVersion version()
    {
        return ServerVersion.version( driver() );
    }

    public void ensureProcedures( String jarName ) throws IOException
    {
        File procedureJar = new File( runner.pluginsDirectory(), jarName );
        if ( !procedureJar.exists() )
        {
            FileTools.copyFile( new File( TEST_RESOURCE_FOLDER_PATH, jarName ), procedureJar );
            debug( "Added a new procedure `%s`", jarName );
            runner.restartNeo4j(); // needs to force to restart as no configuration changed
        }
    }
}
