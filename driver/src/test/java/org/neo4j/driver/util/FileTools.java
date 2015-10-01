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
package org.neo4j.driver.util;

import org.rauschig.jarchivelib.ArchiveStream;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Scanner;

import static java.io.File.createTempFile;

public class FileTools
{
    public static void deleteRecursively( File file )
    {
        if ( file.isDirectory() )
        {
            File[] files = file.listFiles();
            if ( files != null )
            {
                for ( File sub : files )
                {
                    deleteRecursively( sub );
                }
            }
        }

        //noinspection ResultOfMethodCallIgnored
        file.delete();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static File tmpDir() throws IOException
    {
        File tmp = createTempFile( "neo", "compliance" );
        tmp.delete();
        tmp.mkdir();
        return tmp;
    }


    public static void setProperty( File propFile, String name, String value ) throws FileNotFoundException
    {
        boolean foundProp = false;
        Scanner in = new Scanner( propFile );

        File newPropFile = new File( propFile.getParentFile(), "prop.tmp" );
        PrintWriter out = new PrintWriter( newPropFile );

        while ( in.hasNextLine() )
        {
            String line = in.nextLine();
            if ( !line.trim().startsWith( "#" ) )
            {
                String[] tokens = line.split( "=" );
                if ( tokens.length == 2 && tokens[0].equals( name ) )
                {
                    // found property and set it to the new value
                    out.println( name + "=" + value );
                    foundProp = true;
                }
                else
                {
                    // not the property that we are looking for, print it as original
                    out.println( line );
                }
            }
            else
            {
                // comments, print as original
                out.println( line );
            }
        }

        if ( !foundProp )
        {
            // add this as a new prop
            out.println( name + "=" + value );
        }

        in.close();
        out.close();

        propFile.delete();
        newPropFile.renameTo( propFile );
    }

    /** To allow retrieving a runnable neo4j jar from the international webbernets, we have this */
    public static void streamFileTo( String url, File target ) throws IOException
    {
        try ( FileOutputStream out = new FileOutputStream( target );
              InputStream in = new URL( url ).openStream() )
        {
            byte[] buffer = new byte[1024];
            int read = in.read( buffer );
            while ( read != -1 )
            {
                if ( read > 0 )
                {
                    out.write( buffer, 0, read );
                }

                read = in.read( buffer );
            }
        }
    }

    public static void extractTarball( File tarball, File outputDir, File outputName ) throws IOException
    {
        Archiver archiver = ArchiverFactory.createArchiver( "tar", "gz" );

        archiver.extract( tarball, outputDir );

        // Rename the extracted file to something predictable (extracted folder may contain build number, date or so)
        try ( ArchiveStream stream = archiver.stream( tarball ) )
        {
            new File( outputDir, stream.getNextEntry().getName() ).renameTo( outputName );
        }
    }
}
