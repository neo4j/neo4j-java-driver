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
package org.neo4j.driver.v1.stress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;

import org.neo4j.driver.internal.logging.ConsoleLogging;
import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.util.ConcurrentSet;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.util.DaemonThreadFactory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

abstract class AbstractStressIT<C extends AbstractContext>
{
    private static final int THREAD_COUNT = Integer.getInteger( "threadCount", 8 );
    private static final int ASYNC_BATCH_SIZE = Integer.getInteger( "asyncBatchSize", 10 );
    private static final int EXECUTION_TIME_SECONDS = Integer.getInteger( "executionTimeSeconds", 20 );
    private static final boolean DEBUG_LOGGING_ENABLED = Boolean.getBoolean( "loggingEnabled" );

    private LoggerNameTrackingLogging logging;
    private ExecutorService executor;

    Driver driver;

    @Before
    public void setUp()
    {
        logging = new LoggerNameTrackingLogging();

        Config config = Config.build()
                .withLogging( logging )
                .withMaxConnectionPoolSize( 100 )
                .withConnectionAcquisitionTimeout( 1, MINUTES )
                .toConfig();

        driver = GraphDatabase.driver( databaseUri(), authToken(), config );

        ThreadFactory threadFactory = new DaemonThreadFactory( getClass().getSimpleName() + "-worker-" );
        executor = Executors.newCachedThreadPool( threadFactory );
    }

    @After
    public void tearDown()
    {
        executor.shutdownNow();
        if ( driver != null )
        {
            driver.close();
        }
    }

    @Test
    public void blockingApiStressTest() throws Throwable
    {
        runStressTest( this::launchBlockingWorkerThreads );
    }

    @Test
    public void asyncApiStressTest() throws Throwable
    {
        // todo: re-enable when async is supported in routing driver
        assumeTrue( "bolt".equalsIgnoreCase( databaseUri().getScheme() ) );

        runStressTest( this::launchAsyncWorkerThreads );
    }

    private void runStressTest( Function<C,List<Future<?>>> threadLauncher ) throws Throwable
    {
        C context = createContext();
        List<Future<?>> resultFutures = threadLauncher.apply( context );

        ResourcesInfo resourcesInfo = sleepAndGetResourcesInfo();
        context.stop();

        Throwable firstError = null;
        for ( Future<?> future : resultFutures )
        {
            try
            {
                assertNull( future.get( 10, TimeUnit.SECONDS ) );
            }
            catch ( Throwable error )
            {
                firstError = withSuppressed( firstError, error );
            }
        }

        printStats( context );

        if ( firstError != null )
        {
            throw firstError;
        }

        verifyResults( context, resourcesInfo );
    }

    abstract URI databaseUri();

    abstract AuthToken authToken();

    abstract C createContext();

    abstract List<BlockingCommand<C>> createTestSpecificBlockingCommands();

    abstract boolean handleWriteFailure( Throwable error, C context );

    abstract void assertExpectedReadQueryDistribution( C context );

    abstract <A extends C> void printStats( A context );

    private List<Future<?>> launchBlockingWorkerThreads( C context )
    {
        List<BlockingCommand<C>> commands = createBlockingCommands();
        List<Future<?>> futures = new ArrayList<>();

        for ( int i = 0; i < THREAD_COUNT; i++ )
        {
            Future<Void> future = launchBlockingWorkerThread( executor, commands, context );
            futures.add( future );
        }

        return futures;
    }

    private List<BlockingCommand<C>> createBlockingCommands()
    {
        List<BlockingCommand<C>> commands = new ArrayList<>();

        commands.add( new BlockingReadQuery<>( driver, false ) );
        commands.add( new BlockingReadQuery<>( driver, true ) );

        commands.add( new BlockingReadQueryInTx<>( driver, false ) );
        commands.add( new BlockingReadQueryInTx<>( driver, true ) );

        commands.add( new BlockingWriteQuery<>( this, driver, false ) );
        commands.add( new BlockingWriteQuery<>( this, driver, true ) );

        commands.add( new BlockingWriteQueryInTx<>( this, driver, false ) );
        commands.add( new BlockingWriteQueryInTx<>( this, driver, true ) );

        commands.add( new BlockingWrongQuery<>( driver ) );
        commands.add( new BlockingWrongQueryInTx<>( driver ) );

        commands.add( new BlockingFailingQuery<>( driver ) );
        commands.add( new BlockingFailingQueryInTx<>( driver ) );

        commands.add( new FailedAuth<>( databaseUri(), logging ) );

        commands.addAll( createTestSpecificBlockingCommands() );

        return commands;
    }

    private Future<Void> launchBlockingWorkerThread( ExecutorService executor, List<BlockingCommand<C>> commands,
            C context )
    {
        return executor.submit( () ->
        {
            while ( !context.isStopped() )
            {
                BlockingCommand<C> command = randomOf( commands );
                command.execute( context );
            }
            return null;
        } );
    }

    private List<Future<?>> launchAsyncWorkerThreads( C context )
    {
        List<AsyncCommand<C>> commands = createAsyncCommands();
        List<Future<?>> futures = new ArrayList<>();

        for ( int i = 0; i < THREAD_COUNT; i++ )
        {
            Future<Void> future = launchAsyncWorkerThread( executor, commands, context );
            futures.add( future );
        }

        return futures;
    }

    private List<AsyncCommand<C>> createAsyncCommands()
    {
        List<AsyncCommand<C>> commands = new ArrayList<>();

        commands.add( new AsyncReadQuery<>( driver, false ) );
        commands.add( new AsyncReadQuery<>( driver, true ) );

        commands.add( new AsyncReadQueryInTx<>( driver, false ) );
        commands.add( new AsyncReadQueryInTx<>( driver, true ) );

        commands.add( new AsyncWriteQuery<>( this, driver, false ) );
        commands.add( new AsyncWriteQuery<>( this, driver, true ) );

        commands.add( new AsyncWriteQueryInTx<>( this, driver, false ) );
        commands.add( new AsyncWriteQueryInTx<>( this, driver, true ) );

        commands.add( new AsyncWrongQuery<>( driver ) );
        commands.add( new AsyncWrongQueryInTx<>( driver ) );

        commands.add( new AsyncFailingQuery<>( driver ) );
        commands.add( new AsyncFailingQueryInTx<>( driver ) );

        return commands;
    }

    private Future<Void> launchAsyncWorkerThread( ExecutorService executor, List<AsyncCommand<C>> commands, C context )
    {
        return executor.submit( () ->
        {
            while ( !context.isStopped() )
            {
                CompletableFuture<Void> allCommands = executeAsyncCommands( context, commands, ASYNC_BATCH_SIZE );
                assertNull( allCommands.get() );
            }
            return null;
        } );
    }

    @SuppressWarnings( "unchecked" )
    private CompletableFuture<Void> executeAsyncCommands( C context, List<AsyncCommand<C>> commands, int count )
    {
        CompletableFuture<Void>[] executions = new CompletableFuture[count];
        for ( int i = 0; i < count; i++ )
        {
            AsyncCommand<C> command = randomOf( commands );
            CompletionStage<Void> execution = command.execute( context );
            executions[i] = execution.toCompletableFuture();
        }
        return CompletableFuture.allOf( executions );
    }

    private ResourcesInfo sleepAndGetResourcesInfo() throws InterruptedException
    {
        int halfSleepSeconds = Math.max( 1, EXECUTION_TIME_SECONDS / 2 );
        TimeUnit.SECONDS.sleep( halfSleepSeconds );
        ResourcesInfo resourcesInfo = getResourcesInfo();
        TimeUnit.SECONDS.sleep( halfSleepSeconds );
        return resourcesInfo;
    }

    private ResourcesInfo getResourcesInfo()
    {
        long openFileDescriptorCount = getOpenFileDescriptorCount();
        Set<String> acquiredLoggerNames = logging.getAcquiredLoggerNames();
        return new ResourcesInfo( openFileDescriptorCount, acquiredLoggerNames );
    }

    private void verifyResults( C context, ResourcesInfo resourcesInfo )
    {
        assertNoFileDescriptorLeak( resourcesInfo.openFileDescriptorCount );
        assertNoLoggersLeak( resourcesInfo.acquiredLoggerNames );
        assertExpectedNumberOfNodesCreated( context.getCreatedNodesCount() );
        assertExpectedReadQueryDistribution( context );
    }

    private void assertNoFileDescriptorLeak( long previousOpenFileDescriptors )
    {
        // number of open file descriptors should not go up for more than 20%
        long maxOpenFileDescriptors = (long) (previousOpenFileDescriptors * 1.2);
        long currentOpenFileDescriptorCount = getOpenFileDescriptorCount();
        assertThat( "Unexpectedly high number of open file descriptors",
                currentOpenFileDescriptorCount, lessThanOrEqualTo( maxOpenFileDescriptors ) );
    }

    private void assertNoLoggersLeak( Set<String> previousAcquiredLoggerNames )
    {
        Set<String> currentAcquiredLoggerNames = logging.getAcquiredLoggerNames();
        assertThat( "Unexpected amount of logger instances",
                currentAcquiredLoggerNames, equalTo( previousAcquiredLoggerNames ) );
    }

    private void assertExpectedNumberOfNodesCreated( long expectedCount )
    {
        try ( Session session = driver.session() )
        {
            List<Record> records = session.run( "MATCH (n) RETURN count(n) AS nodesCount" ).list();
            assertEquals( 1, records.size() );
            Record record = records.get( 0 );
            long actualCount = record.get( "nodesCount" ).asLong();
            assertEquals( "Unexpected number of nodes in the database", expectedCount, actualCount );
        }
    }

    private static long getOpenFileDescriptorCount()
    {
        try
        {
            OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
            Method method = osBean.getClass().getDeclaredMethod( "getOpenFileDescriptorCount" );
            method.setAccessible( true );
            return (long) method.invoke( osBean );
        }
        catch ( Throwable t )
        {
            return 0;
        }
    }

    private static Throwable withSuppressed( Throwable firstError, Throwable newError )
    {
        if ( firstError == null )
        {
            return newError;
        }
        firstError.addSuppressed( newError );
        return firstError;
    }

    private static <T> T randomOf( List<T> elements )
    {
        int index = ThreadLocalRandom.current().nextInt( elements.size() );
        return elements.get( index );
    }

    private static class ResourcesInfo
    {
        final long openFileDescriptorCount;
        final Set<String> acquiredLoggerNames;

        ResourcesInfo( long openFileDescriptorCount, Set<String> acquiredLoggerNames )
        {
            this.openFileDescriptorCount = openFileDescriptorCount;
            this.acquiredLoggerNames = acquiredLoggerNames;
        }
    }

    private static class LoggerNameTrackingLogging implements Logging
    {
        private final Set<String> acquiredLoggerNames = new ConcurrentSet<>();

        @Override
        public Logger getLog( String name )
        {
            acquiredLoggerNames.add( name );
            if ( DEBUG_LOGGING_ENABLED )
            {
                return new ConsoleLogging.ConsoleLogger( name, Level.FINE );
            }
            return DevNullLogger.DEV_NULL_LOGGER;
        }

        Set<String> getAcquiredLoggerNames()
        {
            return new HashSet<>( acquiredLoggerNames );
        }
    }
}
