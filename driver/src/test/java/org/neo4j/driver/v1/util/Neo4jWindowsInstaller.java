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

import static java.lang.String.format;

/**
 * Install, start and stop neo4j server on windows
 * Note: Installing neo4j on windows requires the admin rights to add neo4j as a windows service.
 */
public class Neo4jWindowsInstaller extends Neo4jInstaller
{
    private static final String winPackageUrl =
            format( "http://alpha.neohq.net/dist/neo4j-enterprise-%s-windows.zip", version );

    @Override
    public void installNeo4j() throws IOException
    {
        super.installNeo4j();

        runPowershellScript( "install-service" );
    }

    @Override
    public void uninstallNeo4j() throws IOException
    {
        runPowershellScript( "uninstall-service" );
    }

    @Override
    public int startNeo4j() throws IOException
    {
        return runPowershellScript( "start" );
    }

    @Override
    public int stopNeo4j() throws IOException
    {
        return runPowershellScript( "stop" );
    }

    @Override
    String neo4jPackageUrl()
    {
        String url = System.getProperty( "packageUri", winPackageUrl );
        return URI.create( url ).toString();
    }

    @Override
    File neo4jTarball()
    {
        return new File( Neo4jInstaller.neo4jDir, version + ".zip" );
    }

    @Override
    Archiver tarballArchiver()
    {
        return ArchiverFactory.createArchiver( "zip" );
    }

    private int runPowershellScript( String cmd ) throws IOException
    {
        return runCommand(
                "powershell.exe",
                format( "%s %s", new File(neo4jHomeDir, "bin/neo4j.bat").getAbsolutePath(), cmd ) );
    }
}
