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
package org.neo4j.driver.internal.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;
import org.neo4j.driver.internal.packstream.PackOutput;

public final class MessageToByteBufWriter
{
    private MessageToByteBufWriter()
    {
    }

    public static ByteBuf asByteBuf( Message message )
    {
        try
        {
            ByteBuf buf = Unpooled.buffer();
            ByteBufOutput output = new ByteBufOutput( buf );
            new PackStreamMessageFormatV1.Writer( output, output.messageBoundaryHook(), true ).write( message );
            return buf;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static class ByteBufOutput implements PackOutput
    {
        final ByteBuf buf;

        ByteBufOutput( ByteBuf buf )
        {
            this.buf = buf;
        }

        @Override
        public PackOutput flush() throws IOException
        {
            return this;
        }

        @Override
        public PackOutput writeByte( byte value ) throws IOException
        {
            buf.writeByte( value );
            return this;
        }

        @Override
        public PackOutput writeBytes( byte[] data ) throws IOException
        {
            buf.writeBytes( data );
            return this;
        }

        @Override
        public PackOutput writeShort( short value ) throws IOException
        {
            buf.writeShort( value );
            return this;
        }

        @Override
        public PackOutput writeInt( int value ) throws IOException
        {
            buf.writeInt( value );
            return this;
        }

        @Override
        public PackOutput writeLong( long value ) throws IOException
        {
            buf.writeLong( value );
            return this;
        }

        @Override
        public PackOutput writeDouble( double value ) throws IOException
        {
            buf.writeDouble( value );
            return this;
        }

        @Override
        public Runnable messageBoundaryHook()
        {
            return new Runnable()
            {
                @Override
                public void run()
                {
                }
            };
        }
    }
}
