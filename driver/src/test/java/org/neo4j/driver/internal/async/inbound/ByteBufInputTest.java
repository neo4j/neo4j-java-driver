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
package org.neo4j.driver.internal.async.inbound;

import io.netty.buffer.ByteBuf;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ByteBufInputTest
{
    @Test
    public void shouldThrowWhenStartedWithNullBuf()
    {
        ByteBufInput input = new ByteBufInput();

        try
        {
            input.start( null );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NullPointerException.class ) );
        }
    }

    @Test
    public void shouldThrowWhenStartedTwice()
    {
        ByteBufInput input = new ByteBufInput();
        input.start( mock( ByteBuf.class ) );

        try
        {
            input.start( mock( ByteBuf.class ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void shouldDelegateHasMoreData()
    {
        ByteBufInput input = new ByteBufInput();
        ByteBuf buf = mock( ByteBuf.class );
        when( buf.isReadable() ).thenReturn( true );
        input.start( buf );

        assertTrue( input.hasMoreData() );
    }

    @Test
    public void shouldDelegateReadByte()
    {
        ByteBufInput input = new ByteBufInput();
        ByteBuf buf = mock( ByteBuf.class );
        when( buf.readByte() ).thenReturn( (byte) 42 );
        input.start( buf );

        assertEquals( (byte) 42, input.readByte() );
    }

    @Test
    public void shouldDelegateReadShort()
    {
        ByteBufInput input = new ByteBufInput();
        ByteBuf buf = mock( ByteBuf.class );
        when( buf.readShort() ).thenReturn( (short) -42 );
        input.start( buf );

        assertEquals( (short) -42, input.readShort() );
    }

    @Test
    public void shouldDelegateReadInt()
    {
        ByteBufInput input = new ByteBufInput();
        ByteBuf buf = mock( ByteBuf.class );
        when( buf.readInt() ).thenReturn( 15 );
        input.start( buf );

        assertEquals( 15, input.readInt() );
    }

    @Test
    public void shouldDelegateReadLong()
    {
        ByteBufInput input = new ByteBufInput();
        ByteBuf buf = mock( ByteBuf.class );
        when( buf.readLong() ).thenReturn( 4242L );
        input.start( buf );

        assertEquals( 4242L, input.readLong() );
    }

    @Test
    public void shouldDelegateReadDouble()
    {
        ByteBufInput input = new ByteBufInput();
        ByteBuf buf = mock( ByteBuf.class );
        when( buf.readDouble() ).thenReturn( 42.42D );
        input.start( buf );

        assertEquals( 42.42D, input.readDouble(), 0.00001 );
    }

    @Test
    public void shouldDelegateReadBytes()
    {
        ByteBufInput input = new ByteBufInput();
        ByteBuf buf = mock( ByteBuf.class );
        input.start( buf );

        input.readBytes( new byte[10], 3, 5 );

        verify( buf ).readBytes( new byte[10], 3, 5 );
    }

    @Test
    public void shouldDelegatePeekByte()
    {
        ByteBufInput input = new ByteBufInput();
        ByteBuf buf = mock( ByteBuf.class );
        when( buf.getByte( anyInt() ) ).thenReturn( (byte) 42 );
        input.start( buf );

        assertEquals( (byte) 42, input.peekByte() );
    }
}
