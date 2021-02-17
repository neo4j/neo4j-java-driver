/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.stress;

import io.netty.util.internal.ConcurrentSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.IntStream;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.InternalDriver;
import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.util.DaemonThreadFactory;

import static java.util.Collections.nCopies;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.driver.SessionConfig.builder;

@ExtendWith( DumpLogsOnFailureWatcher.class )
abstract class AbstractStressTestBase<C extends AbstractContext>
{
    private static final int THREAD_COUNT = Integer.getInteger( "threadCount", 8 );
    private static final int ASYNC_BATCH_SIZE = Integer.getInteger( "asyncBatchSize", 10 );
    private static final int EXECUTION_TIME_SECONDS = Integer.getInteger( "executionTimeSeconds", 20 );
    private static final boolean DEBUG_LOGGING_ENABLED = Boolean.getBoolean( "loggingEnabled" );

    private static final int BIG_DATA_TEST_NODE_COUNT = Integer.getInteger( "bigDataTestNodeCount", 30_000 );
    private static final int BIG_DATA_TEST_BATCH_SIZE = Integer.getInteger( "bigDataTestBatchSize", 10_000 );

    private LoggerNameTrackingLogging logging;
    private ExecutorService executor;

    InternalDriver driver;

    @BeforeEach
    void setUp()
    {
        logging = new LoggerNameTrackingLogging();

        driver = (InternalDriver) GraphDatabase.driver( databaseUri(), authToken(), config() );

        ThreadFactory threadFactory = new DaemonThreadFactory( getClass().getSimpleName() + "-worker-" );
        executor = Executors.newCachedThreadPool( threadFactory );
    }

    @AfterEach
    void tearDown()
    {
        executor.shutdownNow();
        if ( driver != null )
        {
            driver.close();
        }
    }

    @Test
    void blockingApiStressTest() throws Throwable
    {
        runStressTest( this::launchBlockingWorkerThreads );
    }

    @Test
    void asyncApiStressTest() throws Throwable
    {
        runStressTest( this::launchAsyncWorkerThreads );
    }

    @Test
    void rxApiStressTest() throws Throwable
    {
        assertRxIsAvailable();
        runStressTest( this::launchRxWorkerThreads );
    }

    @Test
    void blockingApiBigDataTest()
    {
        Bookmark bookmark = createNodesBlocking( bigDataTestBatchCount(), BIG_DATA_TEST_BATCH_SIZE, driver );
        readNodesBlocking( driver, bookmark, BIG_DATA_TEST_NODE_COUNT );
    }

    @Test
    void asyncApiBigDataTest() throws Throwable
    {
        Bookmark bookmark = createNodesAsync( bigDataTestBatchCount(), BIG_DATA_TEST_BATCH_SIZE, driver );
        readNodesAsync( driver, bookmark, BIG_DATA_TEST_NODE_COUNT );
    }

    @Test
    void rxApiBigDataTest()
    {
        assertRxIsAvailable();
        Bookmark bookmark = createNodesRx( bigDataTestBatchCount(), BIG_DATA_TEST_BATCH_SIZE, driver );
        readNodesRx( driver, bookmark, BIG_DATA_TEST_NODE_COUNT );
    }

    private void assertRxIsAvailable()
    {
        assumeTrue( driver.supportsMultiDb() );
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
                assertNull( future.get( 10, SECONDS ) );
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

    abstract void dumpLogs();

    abstract URI databaseUri();

    abstract AuthToken authToken();

    abstract Config.ConfigBuilder config( Config.ConfigBuilder builder );

    Config config()
    {
        Config.ConfigBuilder builder = Config.builder()
                .withLogging( logging )
                .withMaxConnectionPoolSize( 100 )
                .withConnectionAcquisitionTimeout( 1, MINUTES );
        return config( builder ).build();
    }

    abstract C createContext();

    List<BlockingCommand<C>> createTestSpecificBlockingCommands() {
        return Collections.emptyList();
    }

    List<AsyncCommand<C>> createTestSpecificAsyncCommands() {
        return Collections.emptyList();
    }

    List<RxCommand<C>> createTestSpecificRxCommands() {
        return Collections.emptyList();
    }

    abstract boolean handleWriteFailure( Throwable error, C context );

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

        commands.add( new BlockingReadQueryWithRetries<>( driver, false ) );
        commands.add( new BlockingReadQueryWithRetries<>( driver, true ) );

        commands.add( new BlockingWriteQueryWithRetries<>( this, driver, false ) );
        commands.add( new BlockingWriteQueryWithRetries<>( this, driver, true ) );

        commands.add( new BlockingWrongQueryWithRetries<>( driver ) );

        commands.add( new BlockingFailingQueryWithRetries<>( driver ) );

        commands.add( new FailedAuth<>( databaseUri(), config() ) );

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

    private List<Future<?>> launchRxWorkerThreads( C context )
    {
        List<RxCommand<C>> commands = createRxCommands();
        List<Future<?>> futures = new ArrayList<>();

        for ( int i = 0; i < THREAD_COUNT; i++ )
        {
            Future<Void> future = launchRxWorkerThread( executor, commands, context );
            futures.add( future );
        }
        return futures;
    }

    private List<RxCommand<C>> createRxCommands()
    {
        List<RxCommand<C>> commands = new ArrayList<>();

        commands.add( new RxReadQueryWithRetries<>( driver, false ) );
        commands.add( new RxReadQueryWithRetries<>( driver, true ) );

        commands.add( new RxWriteQueryWithRetries<>( this, driver, false ) );
        commands.add( new RxWriteQueryWithRetries<>( this, driver, true ) );

        commands.add( new RxFailingQueryWithRetries<>( driver ) );

        commands.addAll( createTestSpecificRxCommands() );

        return commands;
    }

    private Future<Void> launchRxWorkerThread( ExecutorService executor, List<RxCommand<C>> commands, C context )
    {
        return executor.submit( () ->
        {
            while ( !context.isStopped() )
            {
                CompletableFuture<Void> allCommands = executeRxCommands( context, commands, ASYNC_BATCH_SIZE );
                assertNull( allCommands.get() );
            }
            return null;
        } );
    }

    private CompletableFuture<Void> executeRxCommands( C context, List<RxCommand<C>> commands, int count )
    {
        CompletableFuture<Void>[] executions = new CompletableFuture[count];
        for ( int i = 0; i < count; i++ )
        {
            RxCommand<C> command = randomOf( commands );
            CompletionStage<Void> execution = command.execute( context );
            executions[i] = execution.toCompletableFuture();
        }
        return CompletableFuture.allOf( executions );
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

        commands.add( new AsyncReadQueryWithRetries<>( driver, false ) );
        commands.add( new AsyncReadQueryWithRetries<>( driver, true ) );

        commands.add( new AsyncWriteQueryWithRetries<>( this, driver, false ) );
        commands.add( new AsyncWriteQueryWithRetries<>( this, driver, true ) );

        commands.add( new AsyncWrongQueryWithRetries<>( driver ) );

        commands.add( new AsyncFailingQueryWithRetries<>( driver ) );

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
        SECONDS.sleep( halfSleepSeconds );
        ResourcesInfo resourcesInfo = getResourcesInfo();
        SECONDS.sleep( halfSleepSeconds );
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
    }

    private void assertNoFileDescriptorLeak( long previousOpenFileDescriptors )
    {
        System.out.println( "Initially open file descriptors: " + previousOpenFileDescriptors );

        // number of open file descriptors should not go up for more than 50%
        long maxOpenFileDescriptors = (long) (previousOpenFileDescriptors * 1.5);
        long currentOpenFileDescriptorCount = getOpenFileDescriptorCount();
        System.out.println( "Currently open file descriptors: " + currentOpenFileDescriptorCount );

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
            assertEquals( expectedCount, actualCount, "Unexpected number of nodes in the database" );
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

    private static int bigDataTestBatchCount()
    {
        if ( BIG_DATA_TEST_NODE_COUNT < BIG_DATA_TEST_BATCH_SIZE )
        {
            return 1;
        }
        return BIG_DATA_TEST_NODE_COUNT / BIG_DATA_TEST_BATCH_SIZE;
    }

    private static Bookmark createNodesBlocking( int batchCount, int batchSize, Driver driver )
    {
        Bookmark bookmark;

        long start = System.nanoTime();
        try ( Session session = driver.session() )
        {
            for ( int i = 0; i < batchCount; i++ )
            {
                int batchIndex = i;
                session.writeTransaction( tx -> createNodesInTx( tx, batchIndex, batchSize ) );
            }
            bookmark = session.lastBookmark();
        }
        long end = System.nanoTime();
        System.out.println( "Node creation with blocking API took: " + NANOSECONDS.toMillis( end - start ) + "ms" );

        return bookmark;
    }

    private static void readNodesBlocking( Driver driver, Bookmark bookmark, int expectedNodeCount )
    {
        long start = System.nanoTime();
        try ( Session session = driver.session( builder().withBookmarks( bookmark ).build() ) )
        {
            int nodesProcessed = session.readTransaction( tx ->
            {
                Result result = tx.run( "MATCH (n:Node) RETURN n" );

                int nodesSeen = 0;
                while ( result.hasNext() )
                {
                    Node node = result.next().get( 0 ).asNode();
                    nodesSeen++;

                    List<String> labels = Iterables.asList( node.labels() );
                    assertEquals( 2, labels.size() );
                    assertTrue( labels.contains( "Test" ) );
                    assertTrue( labels.contains( "Node" ) );

                    verifyNodeProperties( node );
                }
                return nodesSeen;
            } );

            assertEquals( expectedNodeCount, nodesProcessed );
        }
        long end = System.nanoTime();
        System.out.println( "Reading nodes with blocking API took: " + NANOSECONDS.toMillis( end - start ) + "ms" );
    }

    private static Bookmark createNodesAsync( int batchCount, int batchSize, Driver driver ) throws Throwable
    {
        long start = System.nanoTime();

        AsyncSession session = driver.asyncSession();
        CompletableFuture<Throwable> writeTransactions = completedFuture( null );

        for ( int i = 0; i < batchCount; i++ )
        {
            int batchIndex = i;
            writeTransactions = writeTransactions.thenCompose( ignore ->
                    session.writeTransactionAsync( tx -> createNodesInTxAsync( tx, batchIndex, batchSize ) ) );
        }
        writeTransactions = writeTransactions.exceptionally( error -> error )
                .thenCompose( error -> safeCloseSession( session, error ) );

        Throwable error = Futures.blockingGet( writeTransactions );
        if ( error != null )
        {
            throw error;
        }

        long end = System.nanoTime();
        System.out.println( "Node creation with async API took: " + NANOSECONDS.toMillis( end - start ) + "ms" );

        return session.lastBookmark();
    }

    private static void readNodesAsync( Driver driver, Bookmark bookmark, int expectedNodeCount ) throws Throwable
    {
        long start = System.nanoTime();

        AsyncSession session = driver.asyncSession( builder().withBookmarks( bookmark ).build() );
        AtomicInteger nodesSeen = new AtomicInteger();

        CompletionStage<Throwable> readQuery = session.readTransactionAsync( tx ->
                tx.runAsync( "MATCH (n:Node) RETURN n" )
                        .thenCompose( cursor -> cursor.forEachAsync( record ->
                        {
                            Node node = record.get( 0 ).asNode();
                            nodesSeen.incrementAndGet();

                            List<String> labels = Iterables.asList( node.labels() );
                            assertEquals( 2, labels.size() );
                            assertTrue( labels.contains( "Test" ) );
                            assertTrue( labels.contains( "Node" ) );

                            verifyNodeProperties( node );
                        } ) ) )
                .thenApply( summary -> (Throwable) null )
                .exceptionally( error -> error )
                .thenCompose( error -> safeCloseSession( session, error ) );

        Throwable error = Futures.blockingGet( readQuery );
        if ( error != null )
        {
            throw error;
        }

        assertEquals( expectedNodeCount, nodesSeen.get() );

        long end = System.nanoTime();
        System.out.println( "Reading nodes with async API took: " + NANOSECONDS.toMillis( end - start ) + "ms" );
    }

    private Bookmark createNodesRx( int batchCount, int batchSize, InternalDriver driver )
    {
        long start = System.nanoTime();

        RxSession session = driver.rxSession();

        Flux.concat( Flux.range( 0, batchCount ).map( batchIndex ->
            session.writeTransaction( tx -> createNodesInTxRx( tx, batchIndex, batchSize ) )
        ) ).blockLast(); // throw any error if happened

        long end = System.nanoTime();
        System.out.println( "Node creation with reactive API took: " + NANOSECONDS.toMillis( end - start ) + "ms" );

        return session.lastBookmark();
    }

    private Publisher<Void> createNodesInTxRx( RxTransaction tx, int batchIndex, int batchSize )
    {
        return Flux.concat( Flux.range( 0, batchSize ).map( index -> batchIndex * batchSize + index ).map( nodeIndex -> {
            Query query = createNodeInTxQuery( nodeIndex );
            return Flux.from( tx.run(query).consume() ).then(); // As long as there is no error
        } ) );
    }

    private void readNodesRx( InternalDriver driver, Bookmark bookmark, int expectedNodeCount )
    {
        long start = System.nanoTime();

        RxSession session = driver.rxSession( builder().withBookmarks( bookmark ).build() );
        AtomicInteger nodesSeen = new AtomicInteger();

        Publisher<Void> readQuery = session.readTransaction( tx -> Flux.from( tx.run( "MATCH (n:Node) RETURN n" ).records() ).doOnNext( record -> {
            Node node = record.get( 0 ).asNode();
            nodesSeen.incrementAndGet();

            List<String> labels = Iterables.asList( node.labels() );
            assertEquals( 2, labels.size() );
            assertTrue( labels.contains( "Test" ) );
            assertTrue( labels.contains( "Node" ) );

            verifyNodeProperties( node );
        } ).then() );

        Flux.from( readQuery ).blockLast();

        assertEquals( expectedNodeCount, nodesSeen.get() );

        long end = System.nanoTime();
        System.out.println( "Reading nodes with async API took: " + NANOSECONDS.toMillis( end - start ) + "ms" );
    }

    private static Void createNodesInTx( Transaction tx, int batchIndex, int batchSize )
    {
        for ( int index = 0; index < batchSize; index++ )
        {
            int nodeIndex = batchIndex * batchSize + index;
            createNodeInTx( tx, nodeIndex );
        }
        return null;
    }

    private static void createNodeInTx( Transaction tx, int nodeIndex )
    {
        Query query = createNodeInTxQuery( nodeIndex );
        tx.run(query).consume();
    }

    private static CompletionStage<Throwable> createNodesInTxAsync( AsyncTransaction tx, int batchIndex, int batchSize )
    {
        @SuppressWarnings( "unchecked" )
        CompletableFuture<Void>[] queryFutures = IntStream.range( 0, batchSize )
                .map( index -> batchIndex * batchSize + index )
                .mapToObj( nodeIndex -> createNodeInTxAsync( tx, nodeIndex ) )
                .toArray( CompletableFuture[]::new );

        return CompletableFuture.allOf( queryFutures )
                .thenApply( ignored -> (Throwable) null )
                .exceptionally( error -> error );
    }

    private static CompletableFuture<Void> createNodeInTxAsync( AsyncTransaction tx, int nodeIndex )
    {
        Query query = createNodeInTxQuery( nodeIndex );
        return tx.runAsync(query)
                .thenCompose( ResultCursor::consumeAsync )
                .thenApply( ignore -> (Void) null )
                .toCompletableFuture();
    }

    private static Query createNodeInTxQuery(int nodeIndex )
    {
        String query = "CREATE (n:Test:Node) SET n = $props";
        Map<String,Object> params = singletonMap( "props", createNodeProperties( nodeIndex ) );
        return new Query( query, params );
    }

    private static Map<String,Object> createNodeProperties( int nodeIndex )
    {
        Map<String,Object> result = new HashMap<>();
        result.put( "index", nodeIndex );
        result.put( "name", "name-" + nodeIndex );
        result.put( "surname", "surname-" + nodeIndex );
        result.put( "long-indices", nCopies( 10, (long) nodeIndex ) );
        result.put( "double-indices", nCopies( 10, (double) nodeIndex ) );
        result.put( "booleans", nCopies( 10, nodeIndex % 2 == 0 ) );
        return result;
    }

    private static void verifyNodeProperties( Node node )
    {
        int nodeIndex = node.get( "index" ).asInt();
        assertEquals( "name-" + nodeIndex, node.get( "name" ).asString() );
        assertEquals( "surname-" + nodeIndex, node.get( "surname" ).asString() );
        assertEquals( nCopies( 10, (long) nodeIndex ), node.get( "long-indices" ).asList() );
        assertEquals( nCopies( 10, (double) nodeIndex ), node.get( "double-indices" ).asList() );
        assertEquals( nCopies( 10, nodeIndex % 2 == 0 ), node.get( "booleans" ).asList() );
    }

    private static <T> CompletionStage<T> safeCloseSession( AsyncSession session, T result )
    {
        return session.closeAsync()
                .exceptionally( ignore -> null )
                .thenApply( ignore -> result );
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
        private final Logging consoleLogging = Logging.console( Level.FINE );
        private final Set<String> acquiredLoggerNames = new ConcurrentSet<>();

        @Override
        public Logger getLog( String name )
        {
            acquiredLoggerNames.add( name );
            if ( DEBUG_LOGGING_ENABLED )
            {
                return consoleLogging.getLog( name );
            }
            return DevNullLogger.DEV_NULL_LOGGER;
        }

        Set<String> getAcquiredLoggerNames()
        {
            return new HashSet<>( acquiredLoggerNames );
        }
    }
}
