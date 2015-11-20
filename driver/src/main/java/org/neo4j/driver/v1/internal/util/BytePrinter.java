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
package org.neo4j.driver.v1.internal.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.wrap;

public class BytePrinter
{
    /**
     * Print a full byte array as nicely formatted groups of hex numbers.
     * Output looks like:
     * <p>
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     */
    public static void print( byte[] bytes, PrintStream out )
    {
        print( wrap( bytes ), out, 0, bytes.length );
    }

    /**
     * Print a full byte buffer as nicely formatted groups of hex numbers.
     * Output looks like:
     * <p>
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     *
     * @param bytes
     * @param out
     */
    public static void print( ByteBuffer bytes, PrintStream out )
    {
        print( bytes, out, 0, bytes.limit() );
    }

    /**
     * Print a subsection of a byte buffer as nicely formatted groups of hex numbers.
     * Output looks like:
     * <p>
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     *
     * @param bytes
     * @param out
     */
    public static void print( ByteBuffer bytes, PrintStream out, int offset, int length )
    {
        for ( int i = offset; i < offset + length; i++ )
        {
            print( bytes.get( i ), out );
            if ( (i - offset + 1) % 32 == 0 )
            {
                out.println();
            }
            else if ( (i - offset + 1) % 8 == 0 )
            {
                out.print( "    " );
            }
            else
            {
                out.print( " " );
            }
        }
    }

    /**
     * Print a single byte as a hex number. The number will always be two characters wide.
     *
     * @param b
     * @param out
     */
    public static void print( byte b, PrintStream out )
    {
        out.print( hex( b ) );
    }

    /**
     * This should not be in this class, move to a dedicated ascii-art class when appropriate.
     * <p>
     * Use this to standardize the width of some text output to all be left-justified and space-padded
     * on the right side to fill up the given column width.
     *
     * @param str
     * @param columnWidth
     * @return
     */
    public static String ljust( String str, int columnWidth )
    {
        return String.format( "%-" + columnWidth + "s", str );
    }

    /**
     * This should not be in this class, move to a dedicated ascii-art class when appropriate.
     * <p>
     * Use this to standardize the width of some text output to all be right-justified and space-padded
     * on the left side to fill up the given column width.
     *
     * @param str
     * @param columnWidth
     * @return
     */
    public static String rjust( String str, int columnWidth )
    {
        return String.format( "%" + columnWidth + "s", str );
    }

    /**
     * Convert a single byte to a human-readable hex number. The number will always be two characters wide.
     *
     * @param b
     * @return
     */
    public static String hex( byte b )
    {
        return String.format( "%02x", b );
    }

    /**
     * Convert a subsection of a byte buffer to a human readable string of nicely formatted hex numbers.
     * Output looks like:
     * <p>
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     *
     * @param bytes
     * @param offset
     * @param length
     * @return
     */
    public static String hex( ByteBuffer bytes, int offset, int length )
    {
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps;
            ps = new PrintStream( baos, true, "UTF-8" );
            print( bytes, ps, offset, length );
            return baos.toString( "UTF-8" );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Convert a full byte buffer to a human readable string of nicely formatted hex numbers.
     * Output looks like:
     * <p>
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     *
     * @param bytes
     * @return
     */
    public static String hex( ByteBuffer bytes )
    {
        return hex( bytes, 0, bytes.limit() );
    }

    /**
     * Convert a full byte buffer to a human readable string of nicely formatted hex numbers.
     * Output looks like:
     * <p>
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     *
     * @param bytes
     * @return
     */
    public static String hex( byte[] bytes )
    {
        return hex( wrap( bytes ) );
    }

    public static byte[] hexStringToBytes( String s )
    {
        int len = s.length();
        ByteArrayOutputStream data = new ByteArrayOutputStream( 1024 );
        for ( int i = 0; i < len;  )
        {
            int firstDigit = Character.digit( s.charAt( i ), 16 );
            if ( firstDigit != -1 )
            {
                int secondDigit = Character.digit( s.charAt( i + 1 ), 16 );
                int toWrite = (firstDigit << 4) + secondDigit;
                data.write( toWrite );
                i += 2;
            }
            else
            {
                i += 1;
            }
        }
        return data.toByteArray();
    }

    public static String hexInOneLine( ByteBuffer bytes, int offset, int length )
    {
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream out;
            out = new PrintStream( baos, true, "UTF-8" );
            for ( int i = offset; i < offset + length; i++ )
            {
                print( bytes.get( i ), out );
                if ( i == offset + length - 1 )
                {
                    // no pending blanks
                }
                else if ( (i - offset + 1) % 8 == 0 )
                {
                    out.print( "    " );
                }
                else
                {
                    out.print( " " );
                }
            }
            return baos.toString( "UTF-8" );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( e );
        }
    }
}
