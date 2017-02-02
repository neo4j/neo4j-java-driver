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

package org.neo4j.driver.v1.hydration;

import org.neo4j.driver.internal.packstream.Constants;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.packstream.PackStream;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.v1.Value;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Integer.toHexString;
import static java.lang.String.format;
import static org.neo4j.driver.internal.packstream.Constants.UTF_8;
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
        PackStreamType type = peekNextType();
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
            long size = unpackStructureSize();
            byte signature = unpackStructureSignature();
            try
            {
                return hydrant.hydrateStructure(size, signature);
            }
            catch (HydrationException ex)
            {
                throw new IOException(ex.getMessage());
            }
        }
        }
        throw new IOException("Unknown value type: " + type);
    }

    public int unpackStructureSize() throws IOException
    {
        final byte markerByte = in.readByte();
        final byte markerHighNibble = (byte) (markerByte & 0xF0);
        final byte markerLowNibble = (byte) (markerByte & 0x0F);

        if (markerHighNibble == Constants.TINY_STRUCT)
        {
            return markerLowNibble;
        }
        switch (markerByte)
        {
        case Constants.STRUCT_8:
            return unpackUINT8();
        case Constants.STRUCT_16:
            return unpackUINT16();
        default:
            throw new PackStream.Unexpected("Expected a struct, but got: " + toHexString(markerByte));
        }
    }

    public byte unpackStructureSignature() throws IOException
    {
        return in.readByte();
    }

    public StructureHeader unpackStructureHeader() throws IOException
    {
        final byte markerByte = in.readByte();
        final byte markerHighNibble = (byte) (markerByte & 0xF0);
        final byte markerLowNibble = (byte) (markerByte & 0x0F);
        int size;
        if (markerHighNibble == Constants.TINY_STRUCT)
        {
            size = markerLowNibble;
        }
        else
        {
            switch (markerByte)
            {
            case Constants.STRUCT_8:
                size = unpackUINT8();
            case Constants.STRUCT_16:
                size = unpackUINT16();
            default:
                throw new PackStream.Unexpected("Expected a struct, but got: " + toHexString(markerByte));
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

        if (markerHighNibble == Constants.TINY_LIST)
        {
            return markerLowNibble;
        }
        switch (markerByte)
        {
        case Constants.LIST_8:
            return unpackUINT8();
        case Constants.LIST_16:
            return unpackUINT16();
        case Constants.LIST_32:
            long listSize = unpackUINT32();
            if (listSize > Integer.MAX_VALUE)
            {
                throw new PackStream.Overflow(format("Lists cannot contain more than %d items", Integer.MAX_VALUE));
            }
            return (int) listSize;
        default:
            throw new PackStream.Unexpected("Expected a list, but got: " + toHexString(markerByte & 0xFF));
        }
    }

    public long unpackMapHeader() throws IOException
    {
        final byte markerByte = in.readByte();
        final byte markerHighNibble = (byte) (markerByte & 0xF0);
        final byte markerLowNibble = (byte) (markerByte & 0x0F);

        if (markerHighNibble == Constants.TINY_MAP)
        {
            return markerLowNibble;
        }
        switch (markerByte)
        {
        case Constants.MAP_8:
            return unpackUINT8();
        case Constants.MAP_16:
            return unpackUINT16();
        case Constants.MAP_32:
            return unpackUINT32();
        default:
            throw new PackStream.Unexpected("Expected a map, but got: " + toHexString(markerByte));
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
        if (markerByte >= Constants.MINUS_2_TO_THE_4)
        {
            return markerByte;
        }
        switch (markerByte)
        {
        case Constants.INT_8:
            return in.readByte();
        case Constants.INT_16:
            return in.readShort();
        case Constants.INT_32:
            return in.readInt();
        case Constants.INT_64:
            return in.readLong();
        default:
            throw new PackStream.Unexpected("Expected an integer, but got: " + toHexString(markerByte));
        }
    }

    public double unpackDouble() throws IOException
    {
        final byte markerByte = in.readByte();
        if (markerByte == Constants.FLOAT_64)
        {
            return in.readDouble();
        }
        throw new PackStream.Unexpected("Expected a double, but got: " + toHexString(markerByte));
    }

    public String unpackString() throws IOException
    {
        final byte markerByte = in.readByte();
        if (markerByte == Constants.TINY_STRING) // Note no mask, so we compare to 0x80.
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
        case Constants.BYTES_8:
            return unpackBytes(unpackUINT8());
        case Constants.BYTES_16:
            return unpackBytes(unpackUINT16());
        case Constants.BYTES_32:
        {
            long size = unpackUINT32();
            if (size <= Integer.MAX_VALUE)
            {
                return unpackBytes((int) size);
            }
            else
            {
                throw new PackStream.Overflow("BYTES_32 too long for Java");
            }
        }
        default:
            throw new PackStream.Unexpected("Expected binary data, but got: 0x" + toHexString(markerByte & 0xFF));
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
        if (markerByte != Constants.NULL)
        {
            throw new PackStream.Unexpected("Expected a null, but got: 0x" + toHexString(markerByte & 0xFF));
        }
        return null;
    }

    private byte[] unpackUtf8(byte markerByte) throws IOException
    {
        final byte markerHighNibble = (byte) (markerByte & 0xF0);
        final byte markerLowNibble = (byte) (markerByte & 0x0F);

        if (markerHighNibble == Constants.TINY_STRING)
        {
            return unpackBytes(markerLowNibble);
        }
        switch (markerByte)
        {
        case Constants.STRING_8:
            return unpackBytes(unpackUINT8());
        case Constants.STRING_16:
            return unpackBytes(unpackUINT16());
        case Constants.STRING_32:
        {
            long size = unpackUINT32();
            if (size <= Integer.MAX_VALUE)
            {
                return unpackBytes((int) size);
            }
            else
            {
                throw new PackStream.Overflow("STRING_32 too long for Java");
            }
        }
        default:
            throw new PackStream.Unexpected("Expected a string, but got: 0x" + toHexString(markerByte & 0xFF));
        }
    }

    public boolean unpackBoolean() throws IOException
    {
        final byte markerByte = in.readByte();
        switch (markerByte)
        {
        case Constants.TRUE:
            return true;
        case Constants.FALSE:
            return false;
        default:
            throw new PackStream.Unexpected("Expected a boolean, but got: 0x" + toHexString(markerByte & 0xFF));
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

    public PackStreamType peekNextType() throws IOException
    {
        final byte markerByte = in.peekByte();
        final byte markerHighNibble = (byte) (markerByte & 0xF0);

        switch (markerHighNibble)
        {
        case Constants.TINY_STRING:
            return PackStreamType.STRING;
        case Constants.TINY_LIST:
            return PackStreamType.LIST;
        case Constants.TINY_MAP:
            return PackStreamType.MAP;
        case Constants.TINY_STRUCT:
            return PackStreamType.STRUCT;
        }

        switch (markerByte)
        {
        case Constants.NULL:
            return PackStreamType.NULL;
        case Constants.TRUE:
        case Constants.FALSE:
            return PackStreamType.BOOLEAN;
        case Constants.FLOAT_64:
            return PackStreamType.FLOAT;
        case Constants.BYTES_8:
        case Constants.BYTES_16:
        case Constants.BYTES_32:
            return PackStreamType.BYTES;
        case Constants.STRING_8:
        case Constants.STRING_16:
        case Constants.STRING_32:
            return PackStreamType.STRING;
        case Constants.LIST_8:
        case Constants.LIST_16:
        case Constants.LIST_32:
            return PackStreamType.LIST;
        case Constants.MAP_8:
        case Constants.MAP_16:
        case Constants.MAP_32:
            return PackStreamType.MAP;
        case Constants.STRUCT_8:
        case Constants.STRUCT_16:
            return PackStreamType.STRUCT;
        default:
            return PackStreamType.INTEGER;
        }
    }
}
