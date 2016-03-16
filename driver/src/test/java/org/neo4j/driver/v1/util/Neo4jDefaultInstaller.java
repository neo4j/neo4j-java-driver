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

import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * basically mean everything except windows, but actually tested only on linux and osx
 */
public class Neo4jDefaultInstaller extends Neo4jInstaller
{
    private static final String packageUrl =
            String.format( "http://alpha.neohq.net/dist/neo4j-enterprise-%s-unix.tar.gz", version );

    @Override
    public int startNeo4j() throws IOException
    {
        return runScript( "start" );
    }

    @Override
    public int stopNeo4j() throws IOException
    {
        return runScript( "stop" );
    }

    @Override
    String neo4jPackageUrl()
    {
        String url = System.getProperty( "packageUri", packageUrl );
        return URI.create( url ).toString();
    }

    @Override
    File neo4jTarball()
    {
        return new File( Neo4jInstaller.neo4jDir, version + ".tar.gz" );
    }

    @Override
    Archiver tarballArchiver()
    {
        return ArchiverFactory.createArchiver( "tar", "gz" );
    }

    private int runScript( String cmd ) throws IOException
    {
        File scriptFile = new File( neo4jHomeDir, "bin/neo4j" );
        if ( ! scriptFile.setExecutable( true ) )
        {
            throw new IllegalStateException( "Could not set executable permissions for " + scriptFile.getCanonicalPath() );
        }
        return runCommand( scriptFile.getCanonicalPath(), cmd );
    }

}
