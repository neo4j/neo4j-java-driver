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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Arrays.asList;

/**
 * Utility that can be used to temporarily capture and store process-wide stdout and stderr output.
 */
public class StdIOCapture
{
    private final List<String> stdout = new CopyOnWriteArrayList<>();
    private final List<String> stderr = new CopyOnWriteArrayList<>();

    /** Put this in a try-with-resources block to capture all standard io that happens within the try block */
    public AutoCloseable capture()
    {
        final PrintStream originalStdOut = System.out;
        final PrintStream originalStdErr = System.err;
        final ByteArrayOutputStream capturedStdOut = new ByteArrayOutputStream();
        final ByteArrayOutputStream capturedStdErr = new ByteArrayOutputStream();

        System.setOut( new PrintStream( capturedStdOut ) );
        System.setErr( new PrintStream( capturedStdErr ) );

        return () ->
        {
            System.setOut( originalStdOut );
            System.setErr( originalStdErr );
            stdout.addAll( asList( capturedStdOut.toString( "UTF-8" ).split( System.lineSeparator() ) ) );
            stderr.addAll( asList( capturedStdErr.toString( "UTF-8" ).split( System.lineSeparator() ) ) );
        };
    }

    public List<String> stdout()
    {
        return stdout;
    }

    public List<String> stderr()
    {
        return stderr;
    }
}
