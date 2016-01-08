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

import org.rauschig.jarchivelib.Archiver;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.neo4j.driver.v1.util.FileTools.extractTarball;
import static org.neo4j.driver.v1.util.FileTools.streamFileTo;

public abstract class Neo4jInstaller
{

    public static class Neo4jInstallerFactory
    {
        public static Neo4jInstaller create()
        {
            return isWindows() ? new Neo4jWindowsInstaller() : new Neo4jDefaultInstaller();
        }

        public static boolean isWindows()
        {
            String os = System.getProperty( "os.name" );
            return os.startsWith( "Windows" );
        }
    }

    static final String version = System.getProperty( "version", "3.0.0-NIGHTLY" );

    private static final File neo4jDir = new File( "../target/neo4j" );

    public static final File neo4jHomeDir = new File( neo4jDir, version );
    public static final File dbDir = new File( neo4jHomeDir, "data/graph.db" );

    /**
     * download, untar/unzip
     */
    public void installNeo4j() throws IOException
    {
        if ( !neo4jHomeDir.exists() )
        {
            // no neo4j exists
            // download neo4j server from a URL
            File tarball = neo4jTarball();

            ensureDownloaded( tarball, neo4jPackageUrl() );

            // Untar the neo4j server
            System.out.println( "Extracting: " + tarball + " -> " + neo4jDir );
            extractTarball( tarball, neo4jDir, neo4jHomeDir, tarballArchiver() );
        }
    }

    public void uninstallNeo4j() throws IOException
    {
        // doing nothing;
    }

    /**
     * start neo4j
     */
    public abstract int startNeo4j() throws IOException;

    /**
     * stop neo4j
     */
    public abstract int stopNeo4j() throws IOException;

    abstract String neo4jPackageUrl();

    abstract File neo4jTarball();

    abstract Archiver tarballArchiver();

    @SuppressWarnings("LoopStatementThatDoesntLoop")
    public int runCommand( String... cmd ) throws IOException
    {
        ProcessBuilder pb = new ProcessBuilder().inheritIO();
        Map<String,String> env = System.getenv();
        pb.environment().put( "JAVA_HOME",
                // This driver is built to work with multiple java versions.
                // Neo4j, however, works with a specific version of Java. This allows
                // specifying which Java version to use for Neo4j separately from which
                // version to use for the driver tests.
                env.containsKey( "NEO4J_JAVA" ) ? env.get( "NEO4J_JAVA" ) :
                System.getProperties().getProperty( "java.home" ) );
        Process process = pb.command( cmd ).start();
        while (true)
        {
            try
            {
                return process.waitFor();
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
    }

    private void ensureDownloaded( File dst, String srcUrl ) throws IOException
    {
        if ( dst.exists() )
        {
            if ( !dst.delete() )
            {
                throw new IllegalStateException( "Couldn't delete previous download " + dst.getCanonicalPath() );
            }
        }
        File parentFile = dst.getParentFile();
        if ( ! parentFile.exists() && ! parentFile.mkdirs() )
        {
            throw new IllegalStateException( "Couldn't create download directory " + parentFile.getCanonicalPath() );
        }
        System.out.println( "Copying: " + srcUrl + " -> " + dst );
        streamFileTo( srcUrl, dst );
    }

}
