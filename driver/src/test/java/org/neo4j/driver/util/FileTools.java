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

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import static java.io.File.createTempFile;

public class FileTools
{
    private static final int WINDOWS_RETRY_COUNT = 5;

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

    public static File tempFile( String prefix, String suffix ) throws Throwable
    {
        File file = createTempFile( prefix, suffix );
        file.deleteOnExit();
        return file;
    }

    public static File tempFile( String prefix ) throws Throwable
    {
        return tempFile( prefix, ".tmp" );
    }

    public static boolean deleteFile( File file )
    {
        if ( !file.exists() )
        {
            return true;
        }
        int count = 0;
        boolean deleted;
        do
        {
            deleted = file.delete();
            if ( !deleted )
            {
                count++;
                waitAndThenTriggerGC();
            }
        }
        while ( !deleted && count <= WINDOWS_RETRY_COUNT );
        return deleted;
    }

    public static void moveFile( File toMove, File target ) throws IOException
    {
        if ( !toMove.exists() )
        {
            throw new FileNotFoundException( "Source file[" + toMove.getAbsolutePath() + "] not found" );
        }
        if ( target.exists() )
        {
            throw new IOException( "Target file[" + target.getAbsolutePath() + "] already exists" );
        }

        if ( toMove.renameTo( target ) )
        {
            return;
        }

        if ( toMove.isDirectory() )
        {
            Files.createDirectories( target.toPath() );
            copyRecursively( toMove, target, null );
            deleteRecursively( toMove );
        }
        else
        {
            copyFile( toMove, target );
            deleteFile( toMove );
        }
    }

    public static void copyRecursively( File fromDirectory, File toDirectory, FileFilter filter) throws IOException
    {
        for ( File fromFile : fromDirectory.listFiles( filter ) )
        {
            File toFile = new File( toDirectory, fromFile.getName() );
            if ( fromFile.isDirectory() )
            {
                Files.createDirectories( toFile.toPath() );
                copyRecursively( fromFile, toFile, filter );
            }
            else
            {
                copyFile( fromFile, toFile );
            }
        }
    }

    public static void copyFile( File srcFile, File dstFile ) throws IOException
    {
        //noinspection ResultOfMethodCallIgnored
        File parentFile = dstFile.getParentFile();
        if (parentFile!=null)
        {
            parentFile.mkdirs();
        }
        FileInputStream input = null;
        FileOutputStream output = null;
        try
        {
            input = new FileInputStream( srcFile );
            output = new FileOutputStream( dstFile );
            int bufferSize = 1024;
            byte[] buffer = new byte[bufferSize];
            int bytesRead;
            while ( (bytesRead = input.read( buffer )) != -1 )
            {
                output.write( buffer, 0, bytesRead );
            }
        }
        catch ( IOException e )
        {
            // Because the message from this cause may not mention which file it's about
            throw new IOException( "Could not copy '" + srcFile.getCanonicalPath() + "' to '" + dstFile.getCanonicalPath() + "'", e );
        }
        finally
        {
            if ( input != null )
            {
                input.close();
            }
            if ( output != null )
            {
                output.close();
            }
        }
    }

    /*
     * See http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154.
     */
    private static void waitAndThenTriggerGC()
    {
        try
        {
            Thread.sleep( 500 );
        }
        catch ( InterruptedException ee )
        {
            Thread.interrupted();
        } // ok
        System.gc();
    }

    public static void updateProperty( File propFile, String key, String value ) throws IOException
    {
        Map<String, String> propertiesMap = new HashMap<>( 1 );
        propertiesMap.put( key, value );
        updateProperties( propFile, propertiesMap, Collections.<String>emptySet() );
    }

    public static void updateProperties( File propFile, Map<String, String> propertiesMap, Set<String> excludes ) throws IOException
    {
        Scanner in = new Scanner( propFile );

        Set<String> updatedProperties = new HashSet<>( propertiesMap.size() );
        File newPropFile = File.createTempFile( propFile.getName(), null );

        try
        {
            FileOutputStream outStream = new FileOutputStream( newPropFile );
            PrintWriter out = new PrintWriter( outStream );

            while ( in.hasNextLine() )
            {
                String line = in.nextLine();
                if ( !line.trim().startsWith( "#" ) )
                {
                    String[] tokens = line.split( "=" );
                    if ( tokens.length == 2 )
                    {
                        String name = tokens[0].trim();
                        if (excludes.contains( name ))
                        {
                            continue;
                        }

                        Object value = propertiesMap.get( name );
                        if ( value != null && !updatedProperties.contains( name ) )
                        {
                            // found property and set it to the new value
                            printlnProperty( out, name, value );
                            updatedProperties.add( name );
                        }
                        else
                        {
                            // not the property that we are looking for, print it as original
                            out.println( line );
                        }
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

            for ( Map.Entry<String,String> entry : propertiesMap.entrySet() )
            {
                String name = entry.getKey();
                Object value = entry.getValue();
                if ( value != null && !updatedProperties.contains( name ) )
                {
                    // add this as a new prop
                    printlnProperty( out, name, value );
                }
            }

            in.close();
            out.flush();
            out.close();
            deleteFile( propFile );
            moveFile( newPropFile, propFile );
        }
        catch ( IOException | RuntimeException e )
        {
            newPropFile.deleteOnExit();
            throw e;
        }
    }

    private static void printlnProperty( PrintWriter out, String name, Object value )
    {
        out.print( name );
        out.print( '=' );
        out.println( value );
    }
}
