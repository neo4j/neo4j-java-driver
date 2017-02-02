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

import org.neo4j.driver.packstream.io.PackInput;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.hydration.HydrationException;
import org.neo4j.driver.v1.types.GraphHydrant;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Integer.toHexString;
import static java.lang.String.format;
import static org.neo4j.driver.packstream.PackStream.UTF_8;
import static org.neo4j.driver.v1.Values.value;

public class UnpackStream
{
    private static final Map<String,Value> EMPTY_STRING_VALUE_MAP = new HashMap<>();

    private final PackInput in;
    private final GraphHydrant hydrant;

    public UnpackStream(PackInput in)
    {
        this.in = in;
        this.hydrant = new GraphHydrant(this);
    }

    public boolean hasNext() throws IOException
    {
        return in.hasMoreData();
    }

    public Value unpackValue() throws IOException
    {
        PackStream.Type type = peekNextType();
        switch (type)
        {
        case BYTES:
            break;
        case NULL:
            return value(unpackNull());
        case BOOLEAN:
            return value(unpackBoolean());
        case INTEGER:
            return value(unpackLong());
        case FLOAT:
            return value(unpackDouble());
        case STRING:
            return value(unpackString());
        case LIST:
        {
            int size = (int) unpackListHeader();
            Value[] vals = new Value[size];
            for (int j = 0; j < size; j++)
            {
                vals[j] = unpackValue();
            }
            return new ListValue(vals);
        }
        case MAP:
            return new MapValue(unpackMap());
        case STRUCT:
        {
            try
            {
                return hydrant.hydrateStructure(unpackStructureHeader());
            }
            catch (HydrationException ex)
            {
                throw new IOException(ex.getMessage());
            }
        }
        }
        throw new IOException("Unknown value type: " + type);
    }

    public StructureHeader unpackStructureHeader() throws IOException
    {
        final byte markerByte = in.readByte();
        final byte markerHighNibble = (byte) (markerByte & 0xF0);
        final byte markerLowNibble = (byte) (markerByte & 0x0F);
        int size;
        if (markerHighNibble == PackStream.TINY_STRUCT)
        {
            size = markerLowNibble;
        }
        else
        {
            switch (markerByte)
            {
            case PackStream.STRUCT_8:
                size = unpackUINT8();
                break;
            case PackStream.STRUCT_16:
                size = unpackUINT16();
                break;
            default:
                throw new Unexpected("Expected a struct, but got: " + toHexString(markerByte));
            }
        }
        byte signature = in.readByte();
        return new StructureHeader(size, signature);
    }

    public int unpackListHeader() throws IOException
    {
        final byte markerByte = in.readByte();
        final byte markerHighNibble = (byte) (markerByte & 0xF0);
        final byte markerLowNibble = (byte) (markerByte & 0x0F);

        if (markerHighNibble == PackStream.TINY_LIST)
        {
            return markerLowNibble;
        }
        switch (markerByte)
        {
        case PackStream.LIST_8:
            return unpackUINT8();
        case PackStream.LIST_16:
            return unpackUINT16();
        case PackStream.LIST_32:
            long listSize = unpackUINT32();
            if (listSize > Integer.MAX_VALUE)
            {
                throw new Overflow(format("Lists cannot contain more than %d items", Integer.MAX_VALUE));
            }
            return (int) listSize;
        default:
            throw new Unexpected("Expected a list, but got: " + toHexString(markerByte & 0xFF));
        }
    }

    public long unpackMapHeader() throws IOException
    {
        final byte markerByte = in.readByte();
        final byte markerHighNibble = (byte) (markerByte & 0xF0);
        final byte markerLowNibble = (byte) (markerByte & 0x0F);

        if (markerHighNibble == PackStream.TINY_MAP)
        {
            return markerLowNibble;
        }
        switch (markerByte)
        {
        case PackStream.MAP_8:
            return unpackUINT8();
        case PackStream.MAP_16:
            return unpackUINT16();
        case PackStream.MAP_32:
            return unpackUINT32();
        default:
            throw new Unexpected("Expected a map, but got: " + toHexString(markerByte));
        }
    }

    public Map<String, Value> unpackMap() throws IOException
    {
        int size = (int) unpackMapHeader();
        if (size == 0)
        {
            return EMPTY_STRING_VALUE_MAP;
        }
        Map<String, Value> map = new HashMap<>(size);
        for (int i = 0; i < size; i++)
        {
            String key = unpackString();
            map.put(key, unpackValue());
        }
        return map;
    }

    public long unpackLong() throws IOException
    {
        final byte markerByte = in.readByte();
        if (markerByte >= PackStream.MINUS_2_TO_THE_4)
        {
            return markerByte;
        }
        switch (markerByte)
        {
        case PackStream.INT_8:
            return in.readByte();
        case PackStream.INT_16:
            return in.readShort();
        case PackStream.INT_32:
            return in.readInt();
        case PackStream.INT_64:
            return in.readLong();
        default:
            throw new Unexpected("Expected an integer, but got: " + toHexString(markerByte));
        }
    }

    public double unpackDouble() throws IOException
    {
        final byte markerByte = in.readByte();
        if (markerByte == PackStream.FLOAT_64)
        {
            return in.readDouble();
        }
        throw new Unexpected("Expected a double, but got: " + toHexString(markerByte));
    }

    public String unpackString() throws IOException
    {
        final byte markerByte = in.readByte();
        if (markerByte == PackStream.TINY_STRING) // Note no mask, so we compare to 0x80.
        {
            return "";
        }

        return new String(unpackUtf8(markerByte), UTF_8);
    }

    public byte[] unpackBytes() throws IOException
    {
        final byte markerByte = in.readByte();

        switch (markerByte)
        {
        case PackStream.BYTES_8:
            return unpackBytes(unpackUINT8());
        case PackStream.BYTES_16:
            return unpackBytes(unpackUINT16());
        case PackStream.BYTES_32:
        {
            long size = unpackUINT32();
            if (size <= Integer.MAX_VALUE)
            {
                return unpackBytes((int) size);
            }
            else
            {
                throw new Overflow("BYTES_32 too long for Java");
            }
        }
        default:
            throw new Unexpected("Expected binary data, but got: 0x" + toHexString(markerByte & 0xFF));
        }
    }

    /**
     * This may seem confusing. This method exists to move forward the internal pointer when encountering
     * a null value. The idiomatic usage would be someone using {@link #peekNextType()} to detect a null type,
     * and then this method to "skip past it".
     *
     * @return null
     * @throws IOException if the unpacked value was not null
     */
    public Object unpackNull() throws IOException
    {
        final byte markerByte = in.readByte();
        if (markerByte != PackStream.NULL)
        {
            throw new Unexpected("Expected a null, but got: 0x" + toHexString(markerByte & 0xFF));
        }
        return null;
    }

    private byte[] unpackUtf8(byte markerByte) throws IOException
    {
        final byte markerHighNibble = (byte) (markerByte & 0xF0);
        final byte markerLowNibble = (byte) (markerByte & 0x0F);

        if (markerHighNibble == PackStream.TINY_STRING)
        {
            return unpackBytes(markerLowNibble);
        }
        switch (markerByte)
        {
        case PackStream.STRING_8:
            return unpackBytes(unpackUINT8());
        case PackStream.STRING_16:
            return unpackBytes(unpackUINT16());
        case PackStream.STRING_32:
        {
            long size = unpackUINT32();
            if (size <= Integer.MAX_VALUE)
            {
                return unpackBytes((int) size);
            }
            else
            {
                throw new Overflow("STRING_32 too long for Java");
            }
        }
        default:
            throw new Unexpected("Expected a string, but got: 0x" + toHexString(markerByte & 0xFF));
        }
    }

    public boolean unpackBoolean() throws IOException
    {
        final byte markerByte = in.readByte();
        switch (markerByte)
        {
        case PackStream.TRUE:
            return true;
        case PackStream.FALSE:
            return false;
        default:
            throw new Unexpected("Expected a boolean, but got: 0x" + toHexString(markerByte & 0xFF));
        }
    }

    private int unpackUINT8() throws IOException
    {
        return in.readByte() & 0xFF;
    }

    private int unpackUINT16() throws IOException
    {
        return in.readShort() & 0xFFFF;
    }

    private long unpackUINT32() throws IOException
    {
        return in.readInt() & 0xFFFFFFFFL;
    }

    private byte[] unpackBytes(int size) throws IOException
    {
        byte[] heapBuffer = new byte[size];
        in.readBytes(heapBuffer, 0, heapBuffer.length);
        return heapBuffer;
    }

    public PackStream.Type peekNextType() throws IOException
    {
        final byte markerByte = in.peekByte();
        final byte markerHighNibble = (byte) (markerByte & 0xF0);

        switch (markerHighNibble)
        {
        case PackStream.TINY_STRING:
            return PackStream.Type.STRING;
        case PackStream.TINY_LIST:
            return PackStream.Type.LIST;
        case PackStream.TINY_MAP:
            return PackStream.Type.MAP;
        case PackStream.TINY_STRUCT:
            return PackStream.Type.STRUCT;
        }

        switch (markerByte)
        {
        case PackStream.NULL:
            return PackStream.Type.NULL;
        case PackStream.TRUE:
        case PackStream.FALSE:
            return PackStream.Type.BOOLEAN;
        case PackStream.FLOAT_64:
            return PackStream.Type.FLOAT;
        case PackStream.BYTES_8:
        case PackStream.BYTES_16:
        case PackStream.BYTES_32:
            return PackStream.Type.BYTES;
        case PackStream.STRING_8:
        case PackStream.STRING_16:
        case PackStream.STRING_32:
            return PackStream.Type.STRING;
        case PackStream.LIST_8:
        case PackStream.LIST_16:
        case PackStream.LIST_32:
            return PackStream.Type.LIST;
        case PackStream.MAP_8:
        case PackStream.MAP_16:
        case PackStream.MAP_32:
            return PackStream.Type.MAP;
        case PackStream.STRUCT_8:
        case PackStream.STRUCT_16:
            return PackStream.Type.STRUCT;
        default:
            return PackStream.Type.INTEGER;
        }
    }
}
