/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.internal.pool;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.internal.util.Clock;
import org.neo4j.driver.v1.internal.util.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A general pool implementation, heavily inspired by Chris Vests "stormpot" pool, but without a background thread
 * managing allocation.
 * <p>
 * Some quick info to understand this pool:
 * <li>
 * The pool caches a reference for each thread who uses this pool to the resource that has ever been assigned to the
 * thread by the pool.
 * Next time when the same thread wants to get a resource from the pool again, if the cached resource happens to be
 * free, the same resource will be assigned directly to the thread to avoid searching from the global pool.
 * </li>
 * <li>
 * The pool will fail all incoming resource requests once all the resources in the pool has been consumed. But the
 * resource requesting thread could choose to wait for a while for a possible available resource.
 * </li>
 *
 * @param <T> A pool of T
 */
public class ThreadCachingPool<T> implements AutoCloseable
{
    /**
     * Keeps a reference to a locally cached pool slot, to avoid global lookups.
     * Other threads may still access the slot in here, if they cannot acquire an object from the global pool.
     */
    private final ThreadLocal<Slot<T>> local = new ThreadLocal<>();

    /** Keeps references to slots that are likely (but not necessarily) live */
    private final BlockingQueue<Slot<T>> live = new LinkedBlockingQueue<>();

    /** Keeps references to slots that have been disposed of. Used when re-allocating. */
    private final BlockingQueue<Slot<T>> disposed = new LinkedBlockingQueue<>();

    /**
     * All slots in the pool, used when we shut down to dispose of all instances, as well as when there are no known
     * live pool objects, when we use this array to find slots cached by other threads
     */
    private final Slot<T>[] all;

    /** Max number of slots in the pool */
    private final int maxSize;

    /** While the pool is initially populating, this tracks indexes into the {@link #all} array */
    private final AtomicInteger nextSlotIndex = new AtomicInteger( 0 );

    /** Shutdown flag */
    private final AtomicBoolean stopped = new AtomicBoolean( false );

    private final Allocator<T> allocator;
    private final ValidationStrategy<T> validationStrategy;
    private final Clock clock;

    public ThreadCachingPool( int targetSize, Allocator<T> allocator, ValidationStrategy<T> validationStrategy,
            Clock clock )
    {
        this.maxSize = targetSize;
        this.allocator = allocator;
        this.validationStrategy = validationStrategy;
        this.clock = clock;
        this.all = new Slot[targetSize];
    }

    public T acquire( long timeout, TimeUnit unit ) throws InterruptedException
    {
        long deadline = clock.millis() + unit.toMillis( timeout );

        // 1. Try and get an object from our local slot
        Slot<T> slot = local.get();

        if ( slot != null && slot.availableToClaimed() )
        {
            if ( slot.isValid( validationStrategy ) )
            {
                allocator.onAcquire( slot.value );
                return slot.value;
            }
            else
            {
                // We've acquired the slot, but the validation strategy says it's time for it to die. Dispose of it,
                // and go to the global pool.
                dispose( slot );
            }
        }

        // 2. If that fails, acquire from big pool
        return acquireFromGlobal( deadline );
    }

    private T acquireFromGlobal( long deadline ) throws InterruptedException
    {
        Slot<T> slot = live.poll();

        for (; ; )
        {
            if ( stopped.get() )
            {
                throw new IllegalStateException( "Pool has been closed, cannot acquire new values." );
            }

            // 1. Check if the slot we pulled from the live queue is viable
            if ( slot != null )
            {
                // Yay, got a slot - can we keep it?
                if ( slot.availableToClaimed() )
                {
                    if ( slot.isValid( validationStrategy ) )
                    {
                        break;
                    }
                    else
                    {
                        // We've acquired the slot, but the validation strategy says it's time for it to die.
                        dispose( slot );
                    }
                }
            }
            else
            {
                // 2. Exhausted the likely-to-be-live list, are there any disposed-of slots we can recycle?
                slot = disposed.poll();
                if ( slot != null )
                {
                    // Got a hold of a previously disposed slot!
                    slot = allocate( slot.index );
                    break;
                }

                // 3. Can we expand the pool?
                int index = nextSlotIndex.get();
                if ( maxSize > index && nextSlotIndex.compareAndSet( index, index + 1 ) )
                {
                    slot = allocate( index );
                    break;
                }
            }

            // Enforce max wait time
            long timeLeft = deadline - clock.millis();
            if ( timeLeft <= 0 )
            {
                return null;
            }

            // Wait for a bit to see if someone releases something to the live queue
            slot = live.poll( Math.min( timeLeft, 10 ), MILLISECONDS );
        }

        // Keep this slot cached with our thread, so that we can grab this value quickly next time,
        // assuming threads generally availableToClaimed one instance at a time
        local.set( slot );
        allocator.onAcquire( slot.value );
        return slot.value;
    }

    private void dispose( Slot<T> slot )
    {
        if ( !slot.claimedToDisposed() )
        {
            throw new IllegalStateException( "Cannot dispose unclaimed pool object: " + slot );
        }

        // Done before below, in case dispose call fails. This is safe since objects on the
        // pool are used for read-only operations
        disposed.add( slot );
        allocator.onDispose( slot.value );
    }

    /**
     * This method will create allocate a new value, returning the slot in the {@code CLAIMED} state. If allocation
     * fails, a slot for the same index will be added to the {@link #disposed} list, and an exception will be thrown.
     * @param slotIndex the slot index to use for the new item
     * @return a slot in the {@code CLAIMED} state
     */
    private Slot<T> allocate( int slotIndex )
    {
        final Slot<T> slot = new Slot<>( slotIndex, clock );
        try
        {
            // Allocate the new item - this may fail with an exception
            slot.set( allocator.allocate( createDisposeCallback( slot ) ) );

            // Store the slot in the global list of slots
            all[slotIndex] = slot;

            // Return it :)
            return slot;
        }
        catch( Neo4jException e )
        {
            // Failed to allocate slot, return it to the list of disposed slots, rethrow exception.
            slot.claimedToDisposed();
            disposed.add( slot );
            throw e;
        }
    }

    private Consumer<T> createDisposeCallback( final Slot<T> slot )
    {
        return new Consumer<T>()
        {
            @Override
            public void accept( T t )
            {
                slot.updateUsageTimestamp();
                if ( !slot.isValid( validationStrategy ) )
                {
                    // The value has for some reason become invalid, dispose of it
                    dispose( slot );
                    return;
                }

                if ( !slot.claimedToAvailable() )
                {
                    throw new IllegalStateException( "Failed to release pooled object: " + slot );
                }

                // Make sure the pool isn't being stopped in the middle of all these shenanigans
                if ( !stopped.get() )
                {
                    // All good, as you were.
                    live.add( slot );
                }
                else
                {
                    // Another thread concurrently closing the pool may have started closing before we
                    // set our slot to "available". In that case, the slot will not be disposed of by the closing thread
                    // We mitigate this by trying to claim the slot back - if we are able to, we dispose the slot.
                    // If we can't claim the slot back, that means another thread is dealing with it.
                    if ( slot.availableToClaimed() )
                    {
                        dispose( slot );
                    }
                }
            }
        };
    }

    @Override
    public void close()
    {
        if ( !stopped.compareAndSet( false, true ) )
        {
            return;
        }
        for ( Slot<T> slot : all )
        {
            if ( slot != null && slot.availableToClaimed() )
            {
                dispose( slot );
            }
        }
    }
}

/**
 * Stores one pooled resource, along with pooling metadata about it. Every instance the pool manages
 * has one of these objects, independent of if it's currently in use or if it is idle in the pool.
 */
class Slot<T>
{
    enum State
    {
        AVAILABLE,
        CLAIMED,
        DISPOSED
    }

    final AtomicReference<State> state = new AtomicReference<>( State.CLAIMED );
    final int index;
    final Clock clock;

    long lastUsed;
    T value;

    public static <T> Slot<T> disposed( int index, Clock clock )
    {
        Slot<T> slot = new Slot<>( index, clock );
        slot.claimedToDisposed();
        return slot;
    }

    /**
     * @param index the index into the {@link ThreadCachingPool#all all} array, used to re-use that slot when this is
     * disposed
     */
    Slot( int index, Clock clock )
    {
        this.index = index;
        this.clock = clock;
        this.lastUsed = 0;
    }

    public void set( T value )
    {
        this.value = value;
    }

    public boolean availableToClaimed()
    {
        return state.compareAndSet( State.AVAILABLE, State.CLAIMED );
    }

    public boolean claimedToAvailable()
    {
        updateUsageTimestamp();
        return state.compareAndSet( State.CLAIMED, State.AVAILABLE );
    }

    public boolean claimedToDisposed()
    {
        return state.compareAndSet( State.CLAIMED, State.DISPOSED );
    }

    public void updateUsageTimestamp()
    {
        lastUsed = clock.millis();
    }

    boolean isValid( ValidationStrategy<T> strategy )
    {
        return strategy.isValid( value, clock.millis() - lastUsed );
    }

    @Override
    public String toString()
    {
        return "Slot{" +
               "value=" + value +
               ", lastUsed=" + lastUsed +
               ", index=" + index +
               ", state=" + state.get() +
               '}';
    }
}
