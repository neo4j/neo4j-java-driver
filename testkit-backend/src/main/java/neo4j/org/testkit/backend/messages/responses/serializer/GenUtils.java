/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package neo4j.org.testkit.backend.messages.responses.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.List;

@AllArgsConstructor( access = AccessLevel.PRIVATE )
public final class GenUtils
{
    public static void object( JsonGenerator gen, RunnableWithIOException runnable ) throws IOException
    {
        gen.writeStartObject();
        runnable.run();
        gen.writeEndObject();
    }

    public static <T> void list( JsonGenerator gen, List<T> list ) throws IOException
    {
        gen.writeStartArray();
        for ( T element : list )
        {
            gen.writeObject( element );
        }
        gen.writeEndArray();
    }

    public static <T> void cypherObject( JsonGenerator gen, String name, T value ) throws IOException
    {
        cypherObject( gen, name, () -> gen.writeObjectField("value", value ) );
    }

    public static <T> void cypherObject( JsonGenerator gen, String name, RunnableWithIOException runnable ) throws IOException
    {
        object( gen, () ->
        {
            gen.writeStringField( "name", name );
            gen.writeFieldName( "data" );
            object( gen, runnable );
        } );
    }

    interface RunnableWithIOException
    {
        void run() throws IOException;
    }
}
