/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.packstream;

import org.neo4j.driver.packstream.io.BufferedChannelOutput;
import org.neo4j.driver.packstream.io.PackOutput;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

public class PackStream
{
    public enum Type
    {
        NULL,
        BOOLEAN,
        INTEGER,
        FLOAT,
        BYTES,
        STRING,
        LIST,
        MAP,
        STRUCT
    }

    public static final byte TINY_STRING = (byte) 0x80;
    public static final byte TINY_LIST = (byte) 0x90;
    public static final byte TINY_MAP = (byte) 0xA0;
    public static final byte TINY_STRUCT = (byte) 0xB0;
    public static final byte NULL = (byte) 0xC0;
    public static final byte FLOAT_64 = (byte) 0xC1;
    public static final byte FALSE = (byte) 0xC2;
    public static final byte TRUE = (byte) 0xC3;
    public static final byte RESERVED_C4 = (byte) 0xC4;
    public static final byte RESERVED_C5 = (byte) 0xC5;
    public static final byte RESERVED_C6 = (byte) 0xC6;
    public static final byte RESERVED_C7 = (byte) 0xC7;
    public static final byte INT_8 = (byte) 0xC8;
    public static final byte INT_16 = (byte) 0xC9;
    public static final byte INT_32 = (byte) 0xCA;
    public static final byte INT_64 = (byte) 0xCB;
    public static final byte BYTES_8 = (byte) 0xCC;
    public static final byte BYTES_16 = (byte) 0xCD;
    public static final byte BYTES_32 = (byte) 0xCE;
    public static final byte RESERVED_CF = (byte) 0xCF;
    public static final byte STRING_8 = (byte) 0xD0;
    public static final byte STRING_16 = (byte) 0xD1;
    public static final byte STRING_32 = (byte) 0xD2;
    public static final byte RESERVED_D3 = (byte) 0xD3;
    public static final byte LIST_8 = (byte) 0xD4;
    public static final byte LIST_16 = (byte) 0xD5;
    public static final byte LIST_32 = (byte) 0xD6;
    public static final byte RESERVED_D7 = (byte) 0xD7;
    public static final byte MAP_8 = (byte) 0xD8;
    public static final byte MAP_16 = (byte) 0xD9;
    public static final byte MAP_32 = (byte) 0xDA;
    public static final byte RESERVED_DB = (byte) 0xDB;
    public static final byte STRUCT_8 = (byte) 0xDC;
    public static final byte STRUCT_16 = (byte) 0xDD;
    public static final byte RESERVED_DE = (byte) 0xDE; // TODO STRUCT_32? or the class javadoc is wrong?
    public static final byte RESERVED_DF = (byte) 0xDF;
    public static final byte RESERVED_E0 = (byte) 0xE0;
    public static final byte RESERVED_E1 = (byte) 0xE1;
    public static final byte RESERVED_E2 = (byte) 0xE2;
    public static final byte RESERVED_E3 = (byte) 0xE3;
    public static final byte RESERVED_E4 = (byte) 0xE4;
    public static final byte RESERVED_E5 = (byte) 0xE5;
    public static final byte RESERVED_E6 = (byte) 0xE6;
    public static final byte RESERVED_E7 = (byte) 0xE7;
    public static final byte RESERVED_E8 = (byte) 0xE8;
    public static final byte RESERVED_E9 = (byte) 0xE9;
    public static final byte RESERVED_EA = (byte) 0xEA;
    public static final byte RESERVED_EB = (byte) 0xEB;
    public static final byte RESERVED_EC = (byte) 0xEC;
    public static final byte RESERVED_ED = (byte) 0xED;
    public static final byte RESERVED_EE = (byte) 0xEE;
    public static final byte RESERVED_EF = (byte) 0xEF;

    public static final long PLUS_2_TO_THE_31  = 2147483648L;
    public static final long PLUS_2_TO_THE_16  = 65536L;
    public static final long PLUS_2_TO_THE_15  = 32768L;
    public static final long PLUS_2_TO_THE_7   = 128L;
    public static final long MINUS_2_TO_THE_4  = -16L;
    public static final long MINUS_2_TO_THE_7  = -128L;
    public static final long MINUS_2_TO_THE_15 = -32768L;
    public static final long MINUS_2_TO_THE_31 = -2147483648L;

    public static final Charset UTF_8 = Charset.forName( "UTF-8" );

    private PackOutput out;

    public PackStream( PackOutput out )
    {
        this.out = out;
    }

    public void reset( PackOutput out )
    {
        this.out = out;
    }

    public void reset( WritableByteChannel channel )
    {
        ((BufferedChannelOutput) out).reset( channel );
    }

    public void flush() throws IOException
    {
        out.flush();
    }

    public void packRaw( byte[] data ) throws IOException
    {
        out.writeBytes( data, 0, data.length );
    }

    public void packNull() throws IOException
    {
        out.writeByte( NULL );
    }

    public void pack( boolean value ) throws IOException
    {
        out.writeByte( value ? TRUE : FALSE );
    }

    public void pack( long value ) throws IOException
    {
        if ( value >= MINUS_2_TO_THE_4 && value < PLUS_2_TO_THE_7)
        {
            out.writeByte( (byte) value );
        }
        else if ( value >= MINUS_2_TO_THE_7 && value < MINUS_2_TO_THE_4 )
        {
            out.writeByte( INT_8 )
                    .writeByte( (byte) value );
        }
        else if ( value >= MINUS_2_TO_THE_15 && value < PLUS_2_TO_THE_15 )
        {
            out.writeByte( INT_16 )
                    .writeShort( (short) value );
        }
        else if ( value >= MINUS_2_TO_THE_31 && value < PLUS_2_TO_THE_31 )
        {
            out.writeByte( INT_32 )
                    .writeInt( (int) value );
        }
        else
        {
            out.writeByte( INT_64 )
                    .writeLong( value );
        }
    }

    public void pack( double value ) throws IOException
    {
        out.writeByte( FLOAT_64 )
                .writeDouble( value );
    }

    public void pack( byte[] values ) throws IOException
    {
        if ( values == null ) { packNull(); }
        else
        {
            packBytesHeader( values.length );
            packRaw( values );
        }
    }

    public void pack( String value ) throws IOException
    {
        if ( value == null ) { packNull(); }
        else
        {
            byte[] utf8 = value.getBytes( UTF_8 );
            packStringHeader( utf8.length );
            packRaw( utf8 );
        }
    }

    public void packString( byte[] utf8 ) throws IOException
    {
        if ( utf8 == null ) { packNull(); }
        else
        {
            packStringHeader( utf8.length );
            packRaw( utf8 );
        }
    }

    public void pack( List values ) throws IOException
    {
        if ( values == null ) { packNull(); }
        else
        {
            packListHeader( values.size() );
            for ( Object value : values )
            {
                pack( value );
            }
        }
    }

    public void pack( Map values ) throws IOException
    {
        if ( values == null ) { packNull(); }
        else
        {
            packMapHeader( values.size() );
            for ( Object key : values.keySet() )
            {
                pack( key );
                pack( values.get( key ) );
            }
        }
    }

    public void pack( Object value ) throws IOException
    {
        if ( value == null ) { packNull(); }
        else if ( value instanceof Boolean ) { pack( (boolean) value ); }
        else if ( value instanceof boolean[] ) { pack( singletonList( value ) ); }
        else if ( value instanceof Byte ) { pack( (byte) value ); }
        else if ( value instanceof byte[] ) { pack( (byte[]) value ); }
        else if ( value instanceof Short ) { pack( (short) value ); }
        else if ( value instanceof short[] ) { pack( singletonList( value ) ); }
        else if ( value instanceof Integer ) { pack( (int) value ); }
        else if ( value instanceof int[] ) { pack( singletonList( value ) ); }
        else if ( value instanceof Long ) { pack( (long) value ); }
        else if ( value instanceof long[] ) { pack( singletonList( value ) ); }
        else if ( value instanceof Float ) { pack( (float) value ); }
        else if ( value instanceof float[] ) { pack( singletonList( value ) ); }
        else if ( value instanceof Double ) { pack( (double) value ); }
        else if ( value instanceof double[] ) { pack( singletonList( value ) ); }
        else if ( value instanceof Character ) { pack( Character.toString( (char) value ) ); }
        else if ( value instanceof char[] ) { pack( new String( (char[]) value ) ); }
        else if ( value instanceof String ) { pack( (String) value ); }
        else if ( value instanceof String[] ) { pack( singletonList( value ) ); }
        else if ( value instanceof List ) { pack( (List) value ); }
        else if ( value instanceof Map ) { pack( (Map) value ); }
        else { throw new IllegalArgumentException( format( "Cannot pack object %s", value ) );}
    }

    public void packBytesHeader( int size ) throws IOException
    {
        if ( size <= Byte.MAX_VALUE )
        {
            out.writeByte( BYTES_8 )
                    .writeByte( (byte) size );
        }
        else if ( size < PLUS_2_TO_THE_16 )
        {
            out.writeByte( BYTES_16 )
                    .writeShort( (short) size );
        }
        else
        {
            out.writeByte( BYTES_32 )
                    .writeInt( size );
        }
    }

    public void packStringHeader( int size ) throws IOException
    {
        if ( size < 0x10 )
        {
            out.writeByte( (byte) (TINY_STRING | size) );
        }
        else if ( size <= Byte.MAX_VALUE )
        {
            out.writeByte( STRING_8 )
                    .writeByte( (byte) size );
        }
        else if ( size < PLUS_2_TO_THE_16 )
        {
            out.writeByte( STRING_16 )
                    .writeShort( (short) size );
        }
        else
        {
            out.writeByte( STRING_32 )
                    .writeInt( size );
        }
    }

    public void packListHeader( int size ) throws IOException
    {
        if ( size < 0x10 )
        {
            out.writeByte( (byte) (TINY_LIST | size) );
        }
        else if ( size <= Byte.MAX_VALUE )
        {
            out.writeByte( LIST_8 )
                    .writeByte( (byte) size );
        }
        else if ( size < PLUS_2_TO_THE_16 )
        {
            out.writeByte( LIST_16 )
                    .writeShort( (short) size );
        }
        else
        {
            out.writeByte( LIST_32 )
                    .writeInt( size );
        }
    }

    public void packMapHeader( int size ) throws IOException
    {
        if ( size < 0x10 )
        {
            out.writeByte( (byte) (TINY_MAP | size) );
        }
        else if ( size <= Byte.MAX_VALUE )
        {
            out.writeByte( MAP_8 )
                    .writeByte( (byte) size );
        }
        else if ( size < PLUS_2_TO_THE_16 )
        {
            out.writeByte( MAP_16 )
                    .writeShort( (short) size );
        }
        else
        {
            out.writeByte( MAP_32 )
                    .writeInt( size );
        }
    }

    public void packStructHeader( int size, byte signature ) throws IOException
    {
        if ( size < 0x10 )
        {
            out.writeByte( (byte) (TINY_STRUCT | size) )
                    .writeByte( signature );
        }
        else if ( size <= Byte.MAX_VALUE )
        {
            out.writeByte( STRUCT_8 )
                    .writeByte( (byte) size )
                    .writeByte( signature );
        }
        else if ( size < PLUS_2_TO_THE_16 )
        {
            out.writeByte( STRUCT_16 )
                    .writeShort( (short) size )
                    .writeByte( signature );
        }
        else
        {
            long maxSize = PLUS_2_TO_THE_16 - 1;
            throw new Overflow("Structures cannot have more than " + maxSize + " fields" );
        }
    }

}
