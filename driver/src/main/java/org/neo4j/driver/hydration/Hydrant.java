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

package org.neo4j.driver.hydration;

import org.neo4j.driver.packstream.PackStream;
import org.neo4j.driver.packstream.StructureHeader;
import org.neo4j.driver.packstream.UnpackStream;
import org.neo4j.driver.v1.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

/**
 * Hydration is the process of reconstructing native objects from
 * {@link PackStream} structures. The {@link Hydrant} class provides a base
 * on which a client-specific hydration implementation can be built.
 *
 * The only public method exposed is {@link #hydrateStructure(StructureHeader)}}.
 * This method is called by an {@link UnpackStream} when a structure is
 * encountered.
 */
public class Hydrant
{
    private final UnpackStream unpackStream;

    public Hydrant(UnpackStream unpackStream)
    {
        this.unpackStream = unpackStream;
    }

    protected void checkSize(long size, long validSize) throws HydrationException
    {
        if (size != validSize)
        {
            throw new HydrationException(format("Invalid structure size %d", size));
        }
    }

    protected void checkSignature(byte signature, byte expectedSignature) throws HydrationException
    {
        if (signature != expectedSignature)
        {
            throw new HydrationException(format("Expected structure signature %d, found signature %d", expectedSignature, signature));
        }
    }

    protected boolean unpackBoolean() throws IOException
    {
        return unpackStream.unpackBoolean();
    }

    protected long unpackLong() throws IOException
    {
        return unpackStream.unpackLong();
    }

    protected String unpackString() throws IOException
    {
        return unpackStream.unpackString();
    }

    protected int unpackListHeader() throws IOException
    {
        return unpackStream.unpackListHeader();
    }

    protected List<Value> unpackList() throws IOException
    {
        int size = unpackStream.unpackListHeader();
        List<Value> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            list.add(unpackValue());
        }
        return list;
    }

    protected List<String> unpackStringList() throws IOException
    {
        int size = unpackStream.unpackListHeader();
        List<String> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            list.add(unpackString());
        }
        return list;
    }

    protected Map<String, Value> unpackMap() throws IOException
    {
        return unpackStream.unpackMap();
    }

    protected Value unpackValue() throws IOException
    {
        return unpackStream.unpackValue();
    }

    protected StructureHeader unpackStructureHeader() throws IOException
    {
        return unpackStream.unpackStructureHeader();
    }

    public Value hydrateStructure(StructureHeader structureHeader) throws IOException, HydrationException
    {
        throw new HydrationException(format("Unknown structure signature '%s'", structureHeader.signature()));
    }

}
