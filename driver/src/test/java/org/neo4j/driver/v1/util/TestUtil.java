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
package org.neo4j.driver.v1.util;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public final class TestUtil
{
    private TestUtil()
    {
    }

    public static <T, F extends Future<T>> T get( F future )
    {
        if ( !future.isDone() )
        {
            throw new IllegalArgumentException( "Given future is not yet completed" );
        }
        return await( future );
    }

    public static <T, F extends Future<T>> List<T> awaitAll( List<F> futures )
    {
        List<T> result = new ArrayList<>();
        for ( F future : futures )
        {
            result.add( await( future ) );
        }
        return result;
    }

    public static <T, F extends Future<T>> T await( F future )
    {
        try
        {
            return future.get( 1, MINUTES );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new AssertionError( "Interrupted while waiting for future: " + future, e );
        }
        catch ( ExecutionException e )
        {
            Throwable cause = e.getCause();
            StackTraceElement[] originalStackTrace = cause.getStackTrace();
            RuntimeException exceptionWithOriginalStackTrace = new RuntimeException();
            cause.setStackTrace( exceptionWithOriginalStackTrace.getStackTrace() );
            exceptionWithOriginalStackTrace.setStackTrace( originalStackTrace );
            cause.addSuppressed( exceptionWithOriginalStackTrace );
            throwException( cause );
            return null;
        }
        catch ( TimeoutException e )
        {
            throw new AssertionError( "Given future did not complete in time: " + future );
        }
    }

    public static void assertByteBufContains( ByteBuf buf, Number... values )
    {
        try
        {
            assertNotNull( buf );
            int expectedReadableBytes = 0;
            for ( Number value : values )
            {
                expectedReadableBytes += bytesCount( value );
            }
            assertEquals( "Unexpected number of bytes", expectedReadableBytes, buf.readableBytes() );
            for ( Number expectedValue : values )
            {
                Number actualValue = read( buf, expectedValue.getClass() );
                String valueType = actualValue.getClass().getSimpleName();
                assertEquals( valueType + " values not equal", expectedValue, actualValue );
            }
        }
        finally
        {
            buf.release();
        }
    }

    public static void assertByteBufEquals( ByteBuf expected, ByteBuf actual )
    {
        try
        {
            assertEquals( expected, actual );
        }
        finally
        {
            expected.release();
            actual.release();
        }
    }

    private static void throwException( Throwable t )
    {
        TestUtil.<RuntimeException>doThrowException( t );
    }

    @SuppressWarnings( "unchecked" )
    private static <E extends Throwable> void doThrowException( Throwable t ) throws E
    {
        throw (E) t;
    }

    private static Number read( ByteBuf buf, Class<? extends Number> type )
    {
        if ( type == Byte.class )
        {
            return buf.readByte();
        }
        else if ( type == Short.class )
        {
            return buf.readShort();
        }
        else if ( type == Integer.class )
        {
            return buf.readInt();
        }
        else if ( type == Long.class )
        {
            return buf.readLong();
        }
        else if ( type == Float.class )
        {
            return buf.readFloat();
        }
        else if ( type == Double.class )
        {
            return buf.readDouble();
        }
        else
        {
            throw new IllegalArgumentException( "Unexpected numeric type: " + type );
        }
    }

    private static int bytesCount( Number value )
    {
        if ( value instanceof Byte )
        {
            return 1;
        }
        else if ( value instanceof Short )
        {
            return 2;
        }
        else if ( value instanceof Integer )
        {
            return 4;
        }
        else if ( value instanceof Long )
        {
            return 8;
        }
        else if ( value instanceof Float )
        {
            return 4;
        }
        else if ( value instanceof Double )
        {
            return 8;
        }
        else
        {
            throw new IllegalArgumentException(
                    "Unexpected number: '" + value + "' or type" + value.getClass() );
        }
    }
}
