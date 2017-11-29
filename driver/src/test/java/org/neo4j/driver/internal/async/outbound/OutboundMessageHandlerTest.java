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
package org.neo4j.driver.internal.async.outbound;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.EncoderException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.async.ChannelAttributes;
import org.neo4j.driver.internal.async.inbound.ChannelErrorHandler;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.packstream.PackStream;
import org.neo4j.driver.v1.Value;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.async.BoltProtocolV1Util.messageBoundary;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.messaging.MessageFormat.Writer;
import static org.neo4j.driver.internal.messaging.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.v1.Values.value;
import static org.neo4j.driver.v1.util.TestUtil.assertByteBufContains;
import static org.neo4j.driver.v1.util.TestUtil.assertByteBufEquals;

public class OutboundMessageHandlerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @Before
    public void setUp()
    {
        ChannelAttributes.setMessageDispatcher( channel, new InboundMessageDispatcher( channel, DEV_NULL_LOGGING ) );
    }

    @After
    public void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldOutputByteBufAsWrittenByWriterAndMessageBoundary()
    {
        MessageFormat messageFormat = mockMessageFormatWithWriter( 1, 2, 3, 4, 5 );
        OutboundMessageHandler handler = newHandler( messageFormat );
        channel.pipeline().addLast( handler );

        // do not care which message, writer will return predefined bytes anyway
        assertTrue( channel.writeOutbound( PULL_ALL ) );
        assertTrue( channel.finish() );

        assertEquals( 2, channel.outboundMessages().size() );

        ByteBuf buf1 = channel.readOutbound();
        assertByteBufContains( buf1, (short) 5, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5 );

        ByteBuf buf2 = channel.readOutbound();
        assertByteBufEquals( messageBoundary(), buf2 );
    }

    @Test
    public void shouldSupportByteArraysByDefault()
    {
        OutboundMessageHandler handler = newHandler( new PackStreamMessageFormatV1() );
        channel.pipeline().addLast( handler );

        Map<String,Value> params = new HashMap<>();
        params.put( "array", value( new byte[]{1, 2, 3} ) );

        assertTrue( channel.writeOutbound( new RunMessage( "RETURN 1", params ) ) );
        assertTrue( channel.finish() );
    }

    @Test
    public void shouldFailToWriteByteArrayWhenNotSupported()
    {
        OutboundMessageHandler handler = newHandler( new PackStreamMessageFormatV1() ).withoutByteArraySupport();
        channel.pipeline().addLast( handler );
        channel.pipeline().addLast( new ChannelErrorHandler( DEV_NULL_LOGGING ) );

        Map<String,Value> params = new HashMap<>();
        params.put( "array", value( new byte[]{1, 2, 3} ) );

        try
        {
            channel.writeOutbound( new RunMessage( "RETURN 1", params ) );
            fail( "Exception expected" );
        }
        catch ( EncoderException e )
        {
            assertThat( e.getCause(), instanceOf( PackStream.UnPackable.class ) );
        }
    }

    private static MessageFormat mockMessageFormatWithWriter( final int... bytesToWrite )
    {
        MessageFormat messageFormat = mock( MessageFormat.class );

        when( messageFormat.newWriter( any( PackOutput.class ), anyBoolean() ) ).then( invocation ->
        {
            PackOutput output = invocation.getArgumentAt( 0, PackOutput.class );
            return mockWriter( output, bytesToWrite );
        } );

        return messageFormat;
    }

    private static Writer mockWriter( final PackOutput output, final int... bytesToWrite ) throws IOException
    {
        Writer writer = mock( Writer.class );

        doAnswer( invocation ->
        {
            for ( int b : bytesToWrite )
            {
                output.writeByte( (byte) b );
            }
            return writer;
        } ).when( writer ).write( any( Message.class ) );

        return writer;
    }

    private static OutboundMessageHandler newHandler( MessageFormat messageFormat )
    {
        return new OutboundMessageHandler( messageFormat, DEV_NULL_LOGGING );
    }
}
