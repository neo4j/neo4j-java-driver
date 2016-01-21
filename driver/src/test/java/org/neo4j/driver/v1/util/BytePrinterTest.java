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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

import org.neo4j.driver.internal.util.BytePrinter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class BytePrinterTest
{
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    @Test
    public void shouldPrintBytes() throws Throwable
    {
        Assert.assertEquals( "01", BytePrinter.hex( (byte) 1 ) );
        assertEquals( "01 02 03 ", BytePrinter.hex( new byte[]{1, 2, 3} ) );
        assertEquals( "hello     ", BytePrinter.ljust( "hello", 10 ) );
        assertEquals( "     hello", BytePrinter.rjust( "hello", 10 ) );

        BytePrinter.print( (byte) 1, new PrintStream( baos ) );
        assertEquals( "01", new String( baos.toByteArray(), StandardCharsets.UTF_8 ) );

        baos.reset();
        BytePrinter.print( new byte[]{1, 2, 3}, new PrintStream( baos ) );
        assertEquals( "01 02 03 ", new String( baos.toByteArray(), StandardCharsets.UTF_8 ) );

        baos.reset();
        BytePrinter.print( ByteBuffer.wrap( new byte[]{1, 2, 3} ), new PrintStream( baos ) );
        assertEquals( "01 02 03 ", new String( baos.toByteArray(), StandardCharsets.UTF_8 ) );
    }

    @Test
    public void shouldRevertHexStringToBytes()
    {
        byte[] bytes = BytePrinter.hexStringToBytes( "01 02 03    04\n05\n" );
        assertArrayEquals( new byte[]{1, 2, 3, 4, 5}, bytes );
    }
}
