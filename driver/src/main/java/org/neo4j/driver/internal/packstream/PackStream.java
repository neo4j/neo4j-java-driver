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

package org.neo4j.driver.internal.packstream;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

/**
 * PackStream is a messaging serialisation format heavily inspired by MessagePack.
 * The key differences are in the type system itself which (among other things) replaces extensions with structures.
 * The Packer and UnpackStream implementations are also faster than their MessagePack counterparts.
 *
 * Note that several marker byte values are RESERVED for future use.
 * Extra markers should <em>not</em> be added casually and such additions must be follow a strict process involving both client and server software.
 *
 * The table below shows all allocated marker byte values.
 *
 * <table>
 * <tr><th>Marker</th><th>Binary</th><th>Type</th><th>Description</th></tr>
 * <tr><td><code>00..7F</code></td><td><code>0xxxxxxx</code></td><td>+TINY_INT</td><td>Integer 0 to 127</td></tr>
 * <tr><td><code>80..8F</code></td><td><code>1000xxxx</code></td><td>TINY_STRING</td><td></td></tr>
 * <tr><td><code>90..9F</code></td><td><code>1001xxxx</code></td><td>TINY_LIST</td><td></td></tr>
 * <tr><td><code>A0..AF</code></td><td><code>1010xxxx</code></td><td>TINY_MAP</td><td></td></tr>
 * <tr><td><code>B0..BF</code></td><td><code>1011xxxx</code></td><td>TINY_STRUCT</td><td></td></tr>
 * <tr><td><code>C0</code></td><td><code>11000000</code></td><td>NULL</td><td></td></tr>
 * <tr><td><code>C1</code></td><td><code>11000001</code></td><td>FLOAT_64</td><td>64-bit floating point number (double)</td></tr>
 * <tr><td><code>C2</code></td><td><code>11000010</code></td><td>FALSE</td><td>Boolean false</td></tr>
 * <tr><td><code>C3</code></td><td><code>11000011</code></td><td>TRUE</td><td>Boolean true</td></tr>
 * <tr><td><code>C4..C7</code></td><td><code>110001xx</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>C8</code></td><td><code>11001000</code></td><td>INT_8</td><td>8-bit signed integer</td></tr>
 * <tr><td><code>C9</code></td><td><code>11001001</code></td><td>INT_8</td><td>16-bit signed integer</td></tr>
 * <tr><td><code>CA</code></td><td><code>11001010</code></td><td>INT_8</td><td>32-bit signed integer</td></tr>
 * <tr><td><code>CB</code></td><td><code>11001011</code></td><td>INT_8</td><td>64-bit signed integer</td></tr>
 * <tr><td><code>CC</code></td><td><code>11001100</code></td><td>BYTES_8</td><td>Byte string (fewer than 2<sup>8</sup> bytes)</td></tr>
 * <tr><td><code>CD</code></td><td><code>11001101</code></td><td>BYTES_16</td><td>Byte string (fewer than 2<sup>16</sup> bytes)</td></tr>
 * <tr><td><code>CE</code></td><td><code>11001110</code></td><td>BYTES_32</td><td>Byte string (fewer than 2<sup>32</sup> bytes)</td></tr>
 * <tr><td><code>CF</code></td><td><code>11001111</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>D0</code></td><td><code>11010000</code></td><td>STRING_8</td><td>UTF-8 encoded string (fewer than 2<sup>8</sup> bytes)</td></tr>
 * <tr><td><code>D1</code></td><td><code>11010001</code></td><td>STRING_16</td><td>UTF-8 encoded string (fewer than 2<sup>16</sup> bytes)</td></tr>
 * <tr><td><code>D2</code></td><td><code>11010010</code></td><td>STRING_32</td><td>UTF-8 encoded string (fewer than 2<sup>32</sup> bytes)</td></tr>
 * <tr><td><code>D3</code></td><td><code>11010011</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>D4</code></td><td><code>11010100</code></td><td>LIST_8</td><td>List (fewer than 2<sup>8</sup> items)</td></tr>
 * <tr><td><code>D5</code></td><td><code>11010101</code></td><td>LIST_16</td><td>List (fewer than 2<sup>16</sup> items)</td></tr>
 * <tr><td><code>D6</code></td><td><code>11010110</code></td><td>LIST_32</td><td>List (fewer than 2<sup>32</sup> items)</td></tr>
 * <tr><td><code>D7</code></td><td><code>11010111</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>D8</code></td><td><code>11011000</code></td><td>MAP_8</td><td>Map (fewer than 2<sup>8</sup> key:value pairs)</td></tr>
 * <tr><td><code>D9</code></td><td><code>11011001</code></td><td>MAP_16</td><td>Map (fewer than 2<sup>16</sup> key:value pairs)</td></tr>
 * <tr><td><code>DA</code></td><td><code>11011010</code></td><td>MAP_32</td><td>Map (fewer than 2<sup>32</sup> key:value pairs)</td></tr>
 * <tr><td><code>DB</code></td><td><code>11011011</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>DC</code></td><td><code>11011100</code></td><td>STRUCT_8</td><td>Structure (fewer than 2<sup>8</sup> fields)</td></tr>
 * <tr><td><code>DD</code></td><td><code>11011101</code></td><td>STRUCT_16</td><td>Structure (fewer than 2<sup>16</sup> fields)</td></tr>
 * <tr><td><code>DE</code></td><td><code>11011110</code></td><td>STRUCT_32</td><td>Structure (fewer than 2<sup>32</sup> fields)</td></tr>
 * <tr><td><code>DF</code></td><td><code>11011111</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>E0..EF</code></td><td><code>1110xxxx</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>F0..FF</code></td><td><code>1111xxxx</code></td><td>-TINY_INT</td><td>Integer -1 to -16</td></tr>
 * </table>
 *
 */
public class PackStream
{

    private PackStream() {}

    public static class Packer
    {
        private PackOutput out;

        public Packer( PackOutput out )
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
            out.writeByte( Constants.NULL );
        }

        public void pack( boolean value ) throws IOException
        {
            out.writeByte( value ? Constants.TRUE : Constants.FALSE );
        }

        public void pack( long value ) throws IOException
        {
            if ( value >= Constants.MINUS_2_TO_THE_4 && value < Constants.PLUS_2_TO_THE_7)
            {
                out.writeByte( (byte) value );
            }
            else if ( value >= Constants.MINUS_2_TO_THE_7 && value < Constants.MINUS_2_TO_THE_4 )
            {
                out.writeByte( Constants.INT_8 )
                        .writeByte( (byte) value );
            }
            else if ( value >= Constants.MINUS_2_TO_THE_15 && value < Constants.PLUS_2_TO_THE_15 )
            {
                out.writeByte( Constants.INT_16 )
                        .writeShort( (short) value );
            }
            else if ( value >= Constants.MINUS_2_TO_THE_31 && value < Constants.PLUS_2_TO_THE_31 )
            {
                out.writeByte( Constants.INT_32 )
                        .writeInt( (int) value );
            }
            else
            {
                out.writeByte( Constants.INT_64 )
                        .writeLong( value );
            }
        }

        public void pack( double value ) throws IOException
        {
            out.writeByte( Constants.FLOAT_64 )
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
                byte[] utf8 = value.getBytes( Constants.UTF_8 );
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
            else { throw new UnPackable( format( "Cannot pack object %s", value ) );}
        }

        public void packBytesHeader( int size ) throws IOException
        {
            if ( size <= Byte.MAX_VALUE )
            {
                out.writeByte( Constants.BYTES_8 )
                        .writeByte( (byte) size );
            }
            else if ( size < Constants.PLUS_2_TO_THE_16 )
            {
                out.writeByte( Constants.BYTES_16 )
                        .writeShort( (short) size );
            }
            else
            {
                out.writeByte( Constants.BYTES_32 )
                        .writeInt( size );
            }
        }

        public void packStringHeader( int size ) throws IOException
        {
            if ( size < 0x10 )
            {
                out.writeByte( (byte) (Constants.TINY_STRING | size) );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                out.writeByte( Constants.STRING_8 )
                        .writeByte( (byte) size );
            }
            else if ( size < Constants.PLUS_2_TO_THE_16 )
            {
                out.writeByte( Constants.STRING_16 )
                        .writeShort( (short) size );
            }
            else
            {
                out.writeByte( Constants.STRING_32 )
                        .writeInt( size );
            }
        }

        public void packListHeader( int size ) throws IOException
        {
            if ( size < 0x10 )
            {
                out.writeByte( (byte) (Constants.TINY_LIST | size) );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                out.writeByte( Constants.LIST_8 )
                        .writeByte( (byte) size );
            }
            else if ( size < Constants.PLUS_2_TO_THE_16 )
            {
                out.writeByte( Constants.LIST_16 )
                        .writeShort( (short) size );
            }
            else
            {
                out.writeByte( Constants.LIST_32 )
                        .writeInt( size );
            }
        }

        public void packMapHeader( int size ) throws IOException
        {
            if ( size < 0x10 )
            {
                out.writeByte( (byte) (Constants.TINY_MAP | size) );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                out.writeByte( Constants.MAP_8 )
                        .writeByte( (byte) size );
            }
            else if ( size < Constants.PLUS_2_TO_THE_16 )
            {
                out.writeByte( Constants.MAP_16 )
                        .writeShort( (short) size );
            }
            else
            {
                out.writeByte( Constants.MAP_32 )
                        .writeInt( size );
            }
        }

        public void packStructHeader( int size, byte signature ) throws IOException
        {
            if ( size < 0x10 )
            {
                out.writeByte( (byte) (Constants.TINY_STRUCT | size) )
                        .writeByte( signature );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                out.writeByte( Constants.STRUCT_8 )
                        .writeByte( (byte) size )
                        .writeByte( signature );
            }
            else if ( size < Constants.PLUS_2_TO_THE_16 )
            {
                out.writeByte( Constants.STRUCT_16 )
                        .writeShort( (short) size )
                        .writeByte( signature );
            }
            else
            {
                throw new Overflow(
                        "Structures cannot have more than " + (Constants.PLUS_2_TO_THE_16 - 1) + " fields" );
            }
        }

    }

    public static class PackstreamException extends IOException
    {
        private static final long serialVersionUID = -1491422133282345421L;

        protected PackstreamException( String message )
        {
            super( message );
        }
    }

    public static class EndOfStream extends PackstreamException
    {
        private static final long serialVersionUID = 5102836237108105603L;

        public EndOfStream( String message )
        {
            super( message );
        }
    }

    public static class Overflow extends PackstreamException
    {
        private static final long serialVersionUID = -923071934446993659L;

        public Overflow( String message )
        {
            super( message );
        }
    }

    public static class Unexpected extends PackstreamException
    {
        private static final long serialVersionUID = 5004685868740125469L;

        public Unexpected( String message )
        {
            super( message );
        }
    }

    public static class UnPackable extends PackstreamException
    {
        private static final long serialVersionUID = 2408740707769711365L;

        public UnPackable( String message )
        {
            super( message );
        }
    }

}
