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
package org.neo4j.driver.v1.util;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public final class TestUtil
{
    private TestUtil()
    {
    }

    @SafeVarargs
    public static <T> List<T> awaitAll( CompletionStage<T>... stages )
    {
        return awaitAll( Arrays.asList( stages ) );
    }

    public static <T> List<T> awaitAll( List<CompletionStage<T>> stages )
    {
        List<T> result = new ArrayList<>();
        for ( CompletionStage<T> stage : stages )
        {
            result.add( await( stage ) );
        }
        return result;
    }

    public static <T> T await( CompletionStage<T> stage )
    {
        Future<T> future = stage.toCompletableFuture();
        return await( future );
    }

    public static <T> T await( CompletableFuture<T> future )
    {
        return await( (Future<T>) future );
    }

    public static <T, U extends Future<T>> T await( U future )
    {
        try
        {
            return future.get( 5, MINUTES );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new AssertionError( "Interrupted while waiting for future: " + future, e );
        }
        catch ( ExecutionException e )
        {
            PlatformDependent.throwException( e.getCause() );
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
            releaseIfPossible( buf );
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
            releaseIfPossible( expected );
            releaseIfPossible( actual );
        }
    }

    @SafeVarargs
    public static <T> Set<T> asOrderedSet( T... elements )
    {
        return new LinkedHashSet<>( Arrays.asList( elements ) );
    }

    public static void cleanDb( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            cleanDb( session );
        }
    }

    public static void cleanDb( Session session )
    {
        int nodesDeleted;
        do
        {
            nodesDeleted = deleteBatchOfNodes( session );
        }
        while ( nodesDeleted > 0 );
    }

    public static Connection connectionMock()
    {
        Connection connection = mock( Connection.class );
        setupSuccessfulPullAll( connection, "COMMIT" );
        setupSuccessfulPullAll( connection, "ROLLBACK" );
        setupSuccessfulPullAll( connection, "BEGIN" );
        return connection;
    }

    public static void sleep( int millis )
    {
        try
        {
            Thread.sleep( millis );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( e );
        }
    }

    public static void interruptWhenInWaitingState( Thread thread )
    {
        CompletableFuture.runAsync( () ->
        {
            // spin until given thread moves to WAITING state
            do
            {
                sleep( 500 );
            }
            while ( thread.getState() != Thread.State.WAITING );

            thread.interrupt();
        } );
    }

    private static void setupSuccessfulPullAll( Connection connection, String statement )
    {
        doAnswer( invocation ->
        {
            ResponseHandler commitHandler = invocation.getArgumentAt( 3, ResponseHandler.class );
            commitHandler.onSuccess( emptyMap() );
            return null;
        } ).when( connection ).runAndFlush( eq( statement ), any(), any(), any() );
    }

    private static int deleteBatchOfNodes( Session session )
    {
        StatementResult result = session.run( "MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n)" );
        return result.single().get( 0 ).asInt();
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

    private static void releaseIfPossible( ByteBuf buf )
    {
        if ( buf.refCnt() > 0 )
        {
            buf.release();
        }
    }
}
