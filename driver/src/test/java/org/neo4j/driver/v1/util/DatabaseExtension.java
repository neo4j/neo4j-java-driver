/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.v1.util;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Driver;

import static org.neo4j.driver.v1.util.Neo4jRunner.DEFAULT_ADDRESS;
import static org.neo4j.driver.v1.util.Neo4jRunner.DEFAULT_AUTH_TOKEN;
import static org.neo4j.driver.v1.util.Neo4jRunner.DEFAULT_URI;
import static org.neo4j.driver.v1.util.Neo4jRunner.HOME_DIR;
import static org.neo4j.driver.v1.util.Neo4jRunner.debug;
import static org.neo4j.driver.v1.util.Neo4jRunner.getOrCreateGlobalRunner;
import static org.neo4j.driver.v1.util.Neo4jSettings.DEFAULT_TLS_CERT_PATH;
import static org.neo4j.driver.v1.util.Neo4jSettings.DEFAULT_TLS_KEY_PATH;
import static org.neo4j.driver.v1.util.Neo4jSettings.IMPORT_DIR;
import static org.neo4j.driver.v1.util.Neo4jSettings.TEST_SETTINGS;

public class DatabaseExtension implements BeforeEachCallback
{
    static final String TEST_RESOURCE_FOLDER_PATH = "src/test/resources";

    private final Neo4jSettings settings;
    private Neo4jRunner runner;

    public DatabaseExtension()
    {
        this( Neo4jSettings.TEST_SETTINGS );
    }

    private DatabaseExtension( Neo4jSettings settings )
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

    public void forceRestartDb()
    {
        runner.forceToRestart();
    }

    public String createTmpCsvFile( String prefix, String suffix, String... contents ) throws IOException
    {
        File importDir = new File( HOME_DIR, TEST_SETTINGS.propertiesMap().get( IMPORT_DIR ) );
        File tmpFile = File.createTempFile( prefix, suffix, importDir );
        tmpFile.deleteOnExit();

        Files.write( tmpFile.toPath(), Arrays.asList( contents ) );
        return "file:///" + tmpFile.getName();
    }

    public URI uri()
    {
        return DEFAULT_URI;
    }

    public AuthToken authToken()
    {
        return DEFAULT_AUTH_TOKEN;
    }

    public BoltServerAddress address()
    {
        return DEFAULT_ADDRESS;
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


    public void stopDb()
    {
        runner.stopNeo4j();
    }

    public void killDb()
    {
        runner.killNeo4j();
    }

    public ServerVersion version()
    {
        return ServerVersion.version( driver() );
    }
}
