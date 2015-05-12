package org.neo4j.driver.internal.pool;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.util.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.neo4j.driver.internal.util.Clock.SYSTEM;

public class ThreadCachingPoolTest
{
    private final List<PooledObject> inUse = new LinkedList<>();
    private final List<PooledObject> inPool = new LinkedList<>();
    private final List<PooledObject> disposed = new LinkedList<>();

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
    private final Allocator<PooledObject> trackAllocator = new Allocator<PooledObject>()
    {
        @Override
        public PooledObject create( Consumer<PooledObject> release )
        {
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
    };

    @Test
    public void shouldDisposeAllOnClose() throws Throwable
    {
        // Given
        ThreadCachingPool<PooledObject>
                pool = new ThreadCachingPool<>( 4, trackAllocator, checkInvalidateFlag, SYSTEM );

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
                pool = new ThreadCachingPool<>( 4, trackAllocator, checkInvalidateFlag, SYSTEM );

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
                pool = new ThreadCachingPool<>( 1, trackAllocator, checkInvalidateFlag, SYSTEM );

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
                pool = new ThreadCachingPool<>( 4, trackAllocator, invalidIfIdIs(0), SYSTEM );

        // And given we've allocated/releasd object with id 0 once (no validation on first allocation)
        // TODO: Is that the right thing to do? I assume the allocator will allocate healthy objects..
        pool.acquire( 10, TimeUnit.SECONDS ).release();

        // When
        pool.acquire( 10, TimeUnit.SECONDS );

        // Then object with id 0 should've been disposed of, and we should have one live object with id 1
        assertThat( inPool,   equalTo( none() ) );
        assertThat( inUse,    equalTo( items( 1 ) ) );
        assertThat( disposed, equalTo( items( 0 ) ) );
    }

    @Test
    public void shouldNotAllocateNewValuesAfterClose() throws Throwable
    {
        // Given a pool that's been closed
        ThreadCachingPool<PooledObject>
                pool = new ThreadCachingPool<>( 4, trackAllocator, checkInvalidateFlag, SYSTEM );

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
                pool = new ThreadCachingPool<>( 4, trackAllocator, checkInvalidateFlag, SYSTEM );

        PooledObject val = pool.acquire( 10, TimeUnit.SECONDS );

        // When
        val.invalidate().release();

        // Then
        assertThat( inPool,   equalTo( none() ) );
        assertThat( inUse,    equalTo( none() ) );
        assertThat( disposed, equalTo( items( val ) ) );
    }

    private List<PooledObject> items( int ... objects )
    {
        List<PooledObject> out = new LinkedList<>();
        for ( int id : objects )
        {
            out.add( new PooledObject( id, null ) );
        }
        return out;
    }

    private List<PooledObject> items( PooledObject ... objects )
    {
        return Arrays.asList(objects);
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
            this(IDGEN.getAndIncrement(), release);
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
}