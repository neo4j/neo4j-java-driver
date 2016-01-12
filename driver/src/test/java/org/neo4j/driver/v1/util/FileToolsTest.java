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

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class FileToolsTest
{
    @Test
    public void shouldBeAbleToCreateTemporaryDirectory() throws Throwable
    {
        // Given
        File dir = FileTools.tmpDir();

        // Then
        try
        {
            assertThat( dir.exists(), equalTo( true ) );
        }
        finally
        {
            assertThat( FileTools.deleteFile( dir ), equalTo( true ) );
        }
    }

    @Test
    public void shouldAddPropertyAtBottom() throws IOException
    {
        // Given
        File propertyFile = createPropertyFile();

        // When
        FileTools.updateProperty( propertyFile, "cat.name", "mimi" );

        // Then
        try( Scanner in = new Scanner( propertyFile ) )
        {
            assertEquals( "#Wow wow", in.nextLine() );
            assertEquals( "Meow meow", in.nextLine() );
            assertEquals( "color=black", in.nextLine() );
            assertEquals( "cat.age=3", in.nextLine() );
            assertEquals( "cat.name=mimi", in.nextLine() );

            assertFalse( in.hasNextLine() );
        }
        finally
        {
            assertThat( FileTools.deleteFile( propertyFile ), equalTo( true ) );
        }
    }

    @Test
    public void shouldResetPropertyAtTheSameLine() throws IOException
    {
        // Given
        File propertyFile = createPropertyFile();

        // When
        FileTools.updateProperty( propertyFile, "color", "white" );

        // Then
        try( Scanner in = new Scanner( propertyFile ) )
        {
            assertEquals( "#Wow wow", in.nextLine() );
            assertEquals( "Meow meow", in.nextLine() );
            assertEquals( "color=white", in.nextLine() );
            assertEquals( "cat.age=3", in.nextLine() );

            assertFalse( in.hasNextLine() );
        }
        finally
        {
            assertThat( FileTools.deleteFile( propertyFile ), equalTo( true ) );
        }
    }


    private File createPropertyFile() throws FileNotFoundException
    {
        File propFile = new File( "Cat" );
        PrintWriter out = new PrintWriter( propFile );

        out.println( "#Wow wow" );
        out.println( "Meow meow" );
        out.println( "color=black" );
        out.println( "cat.age=3" );

        out.close();
        return propFile;
    }

}
