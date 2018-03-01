/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.driver.internal.messaging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.driver.internal.async.inbound.ByteBufInput;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.util.ByteBufOutput;
import org.neo4j.driver.internal.util.ThrowingConsumer;
import org.neo4j.driver.v1.Value;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1.MSG_RECORD;
import static org.neo4j.driver.internal.packstream.PackStream.FLOAT_64;
import static org.neo4j.driver.internal.packstream.PackStream.Packer;
import static org.neo4j.driver.v1.Values.point2D;
import static org.neo4j.driver.v1.Values.point3D;
import static org.neo4j.driver.v1.util.TestUtil.assertByteBufContains;

public class PackStreamMessageFormatV2Test
{
    private final PackStreamMessageFormatV2 messageFormat = new PackStreamMessageFormatV2();

    @Test
    public void shouldFailToCreateWriterWithoutByteArraySupport()
    {
        PackOutput output = mock( PackOutput.class );

        try
        {
            messageFormat.newWriter( output, false );
            fail( "Exception expected" );
        }
        catch ( IllegalArgumentException ignore )
        {
        }
    }

    @Test
    public void shouldWritePoint2D() throws Exception
    {
        ByteBuf buf = Unpooled.buffer();
        MessageFormat.Writer writer = messageFormat.newWriter( new ByteBufOutput( buf ), true );

        writer.write( new RunMessage( "RETURN $point", singletonMap( "point", point2D( 42, 12.99, -180.0 ) ) ) );

        // point should be encoded as (byte SRID, FLOAT_64 header byte + double X, FLOAT_64 header byte + double Y)
        int index = buf.readableBytes() - Double.BYTES - Byte.BYTES - Double.BYTES - Byte.BYTES - Byte.BYTES;
        ByteBuf tailSlice = buf.slice( index, buf.readableBytes() - index );

        assertByteBufContains( tailSlice, (byte) 42, FLOAT_64, 12.99, FLOAT_64, -180.0 );
    }

    @Test
    public void shouldWritePoint3D() throws Exception
    {
        ByteBuf buf = Unpooled.buffer();
        MessageFormat.Writer writer = messageFormat.newWriter( new ByteBufOutput( buf ), true );

        writer.write( new RunMessage( "RETURN $point", singletonMap( "point", point3D( 42, 0.51, 2.99, 100.123 ) ) ) );

        // point should be encoded as (byte SRID, FLOAT_64 header byte + double X, FLOAT_64 header byte + double Y, FLOAT_64 header byte + double Z)
        int index = buf.readableBytes() - Double.BYTES - Byte.BYTES - Double.BYTES - Byte.BYTES - Double.BYTES - Byte.BYTES - Byte.BYTES;
        ByteBuf tailSlice = buf.slice( index, buf.readableBytes() - index );

        assertByteBufContains( tailSlice, (byte) 42, FLOAT_64, 0.51, FLOAT_64, 2.99, FLOAT_64, 100.123 );
    }

    @Test
    public void shouldReadPoint2D() throws Exception
    {
        InternalPoint2D point = new InternalPoint2D( 42, 120.65, -99.2 );

        testReadingOfPoint( packer ->
        {
            packer.packStructHeader( 3, (byte) 'X' );
            packer.pack( point.srid() );
            packer.pack( point.x() );
            packer.pack( point.y() );
        }, point );
    }

    @Test
    public void shouldReadPoint3D() throws Exception
    {
        InternalPoint3D point = new InternalPoint3D( 42, 85.391, 98.8, 11.1 );

        testReadingOfPoint( packer ->
        {
            packer.packStructHeader( 4, (byte) 'Y' );
            packer.pack( point.srid() );
            packer.pack( point.x() );
            packer.pack( point.y() );
            packer.pack( point.z() );
        }, point );
    }

    private void testReadingOfPoint( ThrowingConsumer<Packer> packAction, Object expected ) throws Exception
    {
        ByteBuf buf = Unpooled.buffer();
        try
        {
            Packer packer = new Packer( new ByteBufOutput( buf ) );
            packer.packStructHeader( 1, MSG_RECORD );
            packer.packListHeader( 1 );
            packAction.accept( packer );

            ByteBufInput input = new ByteBufInput();
            input.start( buf );
            MessageFormat.Reader reader = messageFormat.newReader( input );

            List<Value> values = new ArrayList<>();
            MessageHandler messageHandler = recordMemorizingHandler( values );
            reader.read( messageHandler );
            input.stop();

            assertEquals( 1, values.size() );
            assertEquals( expected, values.get( 0 ).asObject() );
        }
        finally
        {
            buf.release();
        }
    }

    private static MessageHandler recordMemorizingHandler( List<Value> values ) throws IOException
    {
        MessageHandler messageHandler = mock( MessageHandler.class );
        doAnswer( invocation ->
        {
            Value[] arg = invocation.getArgumentAt( 0, Value[].class );
            Collections.addAll( values, arg );
            return null;
        } ).when( messageHandler ).handleRecordMessage( any() );
        return messageHandler;
    }
}
