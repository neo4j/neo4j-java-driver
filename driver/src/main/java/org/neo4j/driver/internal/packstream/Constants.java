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

import org.neo4j.driver.v1.Value;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class Constants
{
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

}
