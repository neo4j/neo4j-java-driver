/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.internal.pool;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.exceptions.ClientException;

import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ThreadCachingPoolTest
{
    private final List<PooledObject> inUse = new LinkedList<>();
    private final List<PooledObject> inPool = new LinkedList<>();
    private final List<PooledObject> disposed = new LinkedList<>();
    private final MethodHandle liveQueueGet = queueGetter( "live" );
    private final MethodHandle disposedQueueGet = queueGetter( "disposed" );
    private final ExecutorService executor = Executors.newFixedThreadPool( 10 );
    private static AtomicInteger IDGEN = new AtomicInteger();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final ValidationStrategy<PooledObject> checkInvalidateFlag = new ValidationStrategy<PooledObject>()
    {
        @Override
        public boolean isValid( PooledObject value, long idleTime )
        {
            return value.valid;
        }
    };

    /** Allocator that allocates pooled objects and tracks their current state (pooled, used, disposed) */
    private final TestAllocator trackAllocator = new TestAllocator();

    @Test
    public void shouldDisposeAllOnClose() throws Throwable
    {
        // Given
        ThreadCachingPool<PooledObject>
                pool = new ThreadCachingPool<>( 4, trackAllocator, checkInvalidateFlag, Clock.SYSTEM );

        PooledObject o1 = pool.acquire( 10, TimeUnit.SECONDS );
        PooledObject o2 = pool.acquire( 10, TimeUnit.SECONDS );

        o1.release();
        o2.release();

        // When
        pool.close();

        // Then
        assertThat( inUse, equalTo( none() ) );
        assertThat( inPool, equalTo( none() ) );
        assertThat( disposed, equalTo( items( o1, o2 ) ) );
    }

    @Test
    public void shouldDisposeValuesReleasedAfterClose() throws Throwable
    {
        // Given
        ThreadCachingPool<PooledObject>
                pool = new ThreadCachingPool<>( 4, trackAllocator, checkInvalidateFlag, Clock.SYSTEM );

        PooledObject o1 = pool.acquire( 10, TimeUnit.SECONDS );
        PooledObject o2 = pool.acquire( 10, TimeUnit.SECONDS );

        o1.release();
        pool.close();

        // When
        o2.release();

        // Then
        assertThat( inUse, equalTo( none() ) );
        assertThat( inPool, equalTo( none() ) );
        assertThat( disposed, equalTo( items( o1, o2 ) ) );
    }

    @Test
    public void shouldBlockUpToTimeoutIfNoneAvailable() throws Throwable
    {
        // Given
        ThreadCachingPool<PooledObject>
                pool = new ThreadCachingPool<>( 1, trackAllocator, checkInvalidateFlag, Clock.SYSTEM );

        pool.acquire( 10, TimeUnit.SECONDS );

        // When
        PooledObject val = pool.acquire( 1, TimeUnit.SECONDS );

        // Then
        assertNull( val );
    }

    @Test
    public void shouldDisposeOfInvalidItems() throws Throwable
    {
        // Given
        ThreadCachingPool<PooledObject>
                pool = new ThreadCachingPool<>( 4, trackAllocator, invalidIfIdIs( 0 ), Clock.SYSTEM );

        // And given we've allocated/releasd object with id 0 once (no validation on first allocation)
        // TODO: Is that the right thing to do? I assume the allocator will allocate healthy objects..
        pool.acquire( 10, TimeUnit.SECONDS ).release();

        // When
        pool.acquire( 10, TimeUnit.SECONDS );

        // Then object with id 0 should've been disposed of, and we should have one live object with id 1
        assertThat( inPool, equalTo( none() ) );
        assertThat( inUse, equalTo( items( 1 ) ) );
        assertThat( disposed, equalTo( items( 0 ) ) );
    }

    @Test
    public void shouldNotAllocateNewValuesAfterClose() throws Throwable
    {
        // Given a pool that's been closed
        ThreadCachingPool<PooledObject>
                pool = new ThreadCachingPool<>( 4, trackAllocator, checkInvalidateFlag, Clock.SYSTEM );

        pool.close();

        // Expect
        exception.expect( IllegalStateException.class );

        // When
        pool.acquire( 10, TimeUnit.SECONDS );
    }

    @Test
    public void shouldDisposeOfObjectsThatBecomeInvalidWhileInUse() throws Throwable
    {
        // Given a pool that's been closed
        ThreadCachingPool<PooledObject>
                pool = new ThreadCachingPool<>( 4, trackAllocator, checkInvalidateFlag, Clock.SYSTEM );

        PooledObject val = pool.acquire( 10, TimeUnit.SECONDS );

        // When
        val.invalidate().release();

        // Then
        assertThat( inPool, equalTo( none() ) );
        assertThat( inUse, equalTo( none() ) );
        assertThat( disposed, equalTo( items( val ) ) );
    }

    @Test
    public void shouldRecoverFromItemCreationFailure() throws Throwable
    {
        // Given a pool where creation will fail from the value-go
        ThreadCachingPool<PooledObject>
                pool = new ThreadCachingPool<>( 4, trackAllocator, checkInvalidateFlag, Clock.SYSTEM );

        trackAllocator.startEmulatingCreationFailures();

        // And given I've acquire a few items, failing to do so
        for ( int i = 0; i < 4; i++ )
        {
            try
            {
                pool.acquire( 10, TimeUnit.SECONDS );
                fail( "Should not succeed at allocating any item here." );
            }
            catch ( ClientException e )
            {
                // Expected
            }
        }

        // When creation starts working again
        trackAllocator.stopEmulatingCreationFailures();

        // Then I should be able to allocate things
        for ( int i = 0; i < 4; i++ )
        {
            pool.acquire( 10, TimeUnit.SECONDS );
        }
        assertThat( inPool, equalTo( none() ) );
        assertThat( inUse, equalTo( items( 0, 1, 2, 3 ) ) );
        assertThat( disposed, equalTo( none() ) ); // because allocation fails, onDispose is not called
    }

    @Test
    public void shouldRecovedDisposedItemReallocationFailing() throws Throwable
    {
        // Given a pool where creation will fail from the value-go
        ThreadCachingPool<PooledObject>
                pool = new ThreadCachingPool<>( 2, trackAllocator, checkInvalidateFlag, Clock.SYSTEM );

        // And given I've allocated and released some stuff, and it became invalid, such that I have a set
        // of disposed-of slots in the pool
        PooledObject first = pool.acquire( 10, TimeUnit.SECONDS );
        PooledObject second = pool.acquire( 10, TimeUnit.SECONDS );
        first.invalidate();
        second.invalidate();
        first.release();
        second.release();

        // And given (bear with me here!) allocation starts failing
        trackAllocator.startEmulatingCreationFailures();

        // And I try and allocate some stuff, failing at it
        for ( int i = 0; i < 2; i++ )
        {
            try
            {
                pool.acquire( 10, TimeUnit.SECONDS );
                fail( "Should not succeed at allocating any item here." );
            }
            catch ( ClientException e )
            {
                // Expected
            }
        }

        // When creation starts working again
        trackAllocator.stopEmulatingCreationFailures();

        // Then I should be able to allocate things
        for ( int i = 0; i < 2; i++ )
        {
            pool.acquire( 10, TimeUnit.SECONDS );
        }
        assertThat( inPool, equalTo( none() ) );
        assertThat( inUse, equalTo( items( 2, 3 ) ) );
        // only the first two items value onDispose called, since allocation fails after that
        assertThat( disposed, equalTo( items( 0, 1 ) ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldNotHaveReferenceAsBothLiveAndDisposed() throws Throwable
    {
        // Given
        final ThreadCachingPool<PooledObject>
                pool = new ThreadCachingPool<>( 4, trackAllocator, checkInvalidateFlag, Clock.SYSTEM );

        // This object will be cached in ThreadLocal
        final PooledObject obj1 = pool.acquire( 10, TimeUnit.SECONDS );

        //This will add another object to the live queue
        assertTrue( acquireInSeparateThread( pool ) );

        //Now we release the first object, meaning that it will be added
        //to the live queue (as well as being cached as ThreadLocal in this thread)
        obj1.release();
        //Now we invalidate the object
        obj1.invalidate();

        // When
        //Now the cached object is invalidated, we should now pick the object
        //from the live objects created in the background thread
        PooledObject obj2 = pool.acquire( 10, TimeUnit.SECONDS );

        //THEN
        assertThat( obj1.id, equalTo( 0 ) );
        assertThat( obj2.id, equalTo( 1 ) );
        BlockingQueue<Slot<PooledObject>> liveQueue = (BlockingQueue<Slot<PooledObject>>) liveQueueGet.invoke( pool );
        BlockingQueue<Slot<PooledObject>> disposedQueue =
                (BlockingQueue<Slot<PooledObject>>) disposedQueueGet.invoke( pool );

        assertThat( disposedQueue, empty() );
        assertThat( liveQueue, hasSize( 1 ) );
        assertThat( liveQueue.poll( 10, TimeUnit.SECONDS ).value.id, equalTo( 0 ) );
    }

    private boolean acquireInSeparateThread( final ThreadCachingPool<PooledObject> pool ) throws InterruptedException
    {
        final AtomicBoolean succeeded = new AtomicBoolean( true );
        executor.execute( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    PooledObject obj = pool.acquire( 10, TimeUnit.MINUTES );
                    obj.release();
                }
                catch ( InterruptedException e )
                {
                    succeeded.set( false );
                }
            }
        } );
        executor.awaitTermination( 2, TimeUnit.SECONDS );
        return succeeded.get();
    }


    //This is terrible hack, but I really want to keep the queues private in
    //ThreadCachingPool
    private static MethodHandle queueGetter( String name )
    {
        try
        {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Field value = ThreadCachingPool.class.getDeclaredField( name );
            value.setAccessible( true );
            return lookup.unreflectGetter( value );
        }
        catch ( NoSuchFieldException | IllegalAccessException e )
        {
            throw new AssertionError( e );
        }
    }

    private List<PooledObject> items( int... objects )
    {
        List<PooledObject> out = new LinkedList<>();
        for ( int id : objects )
        {
            out.add( new PooledObject( id, null ) );
        }
        return out;
    }

    private List<PooledObject> items( PooledObject... objects )
    {
        return Arrays.asList( objects );
    }

    private List<PooledObject> none()
    {
        return Collections.emptyList();
    }

    private ValidationStrategy<PooledObject> invalidIfIdIs( final int i )
    {
        return new ValidationStrategy<PooledObject>()
        {
            @Override
            public boolean isValid( PooledObject value, long idleTime )
            {
                return value.id != i;
            }
        };
    }

    @Before
    public void reset()
    {
        IDGEN.set( 0 );
    }

    private class PooledObject
    {
        private final int id;
        private Consumer<PooledObject> release;
        private boolean valid = true;

        public PooledObject( Consumer<PooledObject> release )
        {
            this( IDGEN.getAndIncrement(), release );
        }

        public PooledObject( int id, Consumer<PooledObject> release )
        {
            this.id = id;
            this.release = release;
        }

        public PooledObject release()
        {
            inUse.remove( this );
            inPool.add( this );
            release.accept( this );
            return this;
        }

        public PooledObject invalidate()
        {
            this.valid = false;
            return this;
        }

        @Override
        public String toString()
        {
            return "PooledObject<" + id + ">";
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

            PooledObject that = (PooledObject) o;

            return id == that.id;

        }

        @Override
        public int hashCode()
        {
            return id;
        }
    }

    private class TestAllocator implements Allocator<PooledObject>
    {
        private ClientException creationException;

        @Override
        public PooledObject allocate( Consumer<PooledObject> release )
        {
            if ( creationException != null )
            {
                throw creationException;
            }
            PooledObject p = new PooledObject( release );
            inPool.add( p );
            return p;
        }

        @Override
        public void onDispose( PooledObject o )
        {
            inPool.remove( o );
            inUse.remove( o );
            disposed.add( o );
        }

        @Override
        public void onAcquire( PooledObject o )
        {
            inPool.remove( o );
            inUse.add( o );
        }

        public void startEmulatingCreationFailures()
        {
            this.creationException = new ClientException( "Failed to create item," );
        }

        public void stopEmulatingCreationFailures()
        {
            this.creationException = null;
        }
    }
}
