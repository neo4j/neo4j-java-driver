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
package org.neo4j.driver.v1.stress;

import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.pool.Allocator;
import org.neo4j.driver.internal.pool.ThreadCachingPool;
import org.neo4j.driver.internal.pool.ValidationStrategy;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.exceptions.Neo4jException;

import static org.junit.Assert.assertFalse;

public class ThreadCachingPoolStressTest
{
    private static final int WORKER_THREADS = 10;
    public static final long TOTAL_MAX_TIME = 10000L;
    private final ExecutorService executor = Executors.newFixedThreadPool( WORKER_THREADS );
    private final AtomicBoolean hasFailed = new AtomicBoolean( false );

    @Test
    public void shouldWorkFine() throws InterruptedException
    {
        // Given
        ThreadCachingPool<PooledObject>
                pool =
                new ThreadCachingPool<>( WORKER_THREADS, new TestAllocator(), checkInvalidateFlag, Clock.SYSTEM );
        // When
        doStuffInTheBackground( pool );
        executor.awaitTermination( TOTAL_MAX_TIME, TimeUnit.MILLISECONDS );
        // Then

        assertFalse( hasFailed.get() );
    }

    private void doStuffInTheBackground( final ThreadCachingPool<PooledObject> pool )
    {
        for ( int i = 0; i < WORKER_THREADS; i++ )
        {
            executor.execute( new Worker( pool ) );
        }
    }

    private class PooledObject
    {
        private boolean valid = true;
        private final Consumer<PooledObject> release;

        private PooledObject( Consumer<PooledObject> release )
        {
            this.release = release;
        }

        void close()
        {
            release.accept( this );
        }

        public void invalidate()
        {
            this.valid = false;
        }
    }

    private class TestAllocator implements Allocator<PooledObject>
    {

        @Override
        public PooledObject allocate( Consumer<PooledObject> release ) throws Neo4jException
        {
            return new PooledObject( release );
        }

        @Override
        public void onDispose( PooledObject o )
        {

        }

        @Override
        public void onAcquire( PooledObject o )
        {

        }
    }

    private final ValidationStrategy<PooledObject> checkInvalidateFlag = new ValidationStrategy<PooledObject>()
    {
        @Override
        public boolean isValid( PooledObject value, long idleTime )
        {
            return value.valid;
        }
    };

    private class Worker implements Runnable
    {
        private final ThreadLocalRandom random;
        private final double probabilityToRelease;
        private final double probabilityToInvalidate;
        private final ThreadCachingPool<PooledObject> pool;
        private final long timeToRun;

        public Worker( ThreadCachingPool<PooledObject> pool )
        {
            this.pool = pool;
            this.random = ThreadLocalRandom.current();
            this.probabilityToRelease = 0.5;
            this.probabilityToInvalidate = 0.5;
            this.timeToRun = random.nextLong( TOTAL_MAX_TIME );
        }

        @Override
        public void run()
        {
            try
            {
                long deadline = timeToRun + System.currentTimeMillis();
                for (; ; )
                {
                    PooledObject object = pool.acquire( random.nextInt( 1 ), TimeUnit.SECONDS );
                    if ( object != null )
                    {


                        Thread.sleep( random.nextInt( 100 ) );
                        object.close();
                        if ( random.nextDouble() < probabilityToInvalidate )
                        {
                            Thread.sleep( random.nextInt( 100 ) );
                            object.invalidate();
                        }
                    }
                    Thread.sleep( random.nextInt( 100 ) );

                    long timeLeft = deadline - System.currentTimeMillis();
                    if ( timeLeft <= 0 )
                    {
                        break;
                    }
                }
            }
            catch ( Throwable e )
            {
                e.printStackTrace();
                hasFailed.set( true );
            }
        }
    }
}
