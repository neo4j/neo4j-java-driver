/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.testutil;

import static java.util.Arrays.asList;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Utility that can be used to temporarily capture and store process-wide stdout and stderr output.
 */
public class StdIOCapture implements AutoCloseable {
    private final List<String> stdout = new CopyOnWriteArrayList<>();
    private final PrintStream originalStdOut;
    private final PrintStream originalStdErr;
    private final ByteArrayOutputStream capturedStdOut;

    /** Put this in a try-with-resources block to capture all standard io that happens within the try block */
    public static StdIOCapture capture() {
        return new StdIOCapture();
    }

    private StdIOCapture() {
        originalStdOut = System.out;
        originalStdErr = System.err;
        capturedStdOut = new ByteArrayOutputStream();
        var capturedStdErr = new ByteArrayOutputStream();

        System.setOut(new PrintStream(capturedStdOut));
        System.setErr(new PrintStream(capturedStdErr));
    }

    public List<String> stdout() {
        return stdout;
    }

    @Override
    public void close() {
        System.setOut(originalStdOut);
        System.setErr(originalStdErr);
        stdout.addAll(asList(capturedStdOut.toString(StandardCharsets.UTF_8).split(System.lineSeparator())));
    }
}
