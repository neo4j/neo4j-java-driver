/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static java.util.Collections.nCopies;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.Values.point;

import java.io.Serial;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransactionContext;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.InternalDriver;
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.neo4j.driver.reactivestreams.ReactiveSession;
import org.neo4j.driver.reactivestreams.ReactiveTransactionContext;
import org.neo4j.driver.testutil.DaemonThreadFactory;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Point;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

abstract class AbstractStressTestBase<C extends AbstractContext> {
    private static final int THREAD_COUNT = Integer.getInteger("threadCount", 8);
    private static final int ASYNC_BATCH_SIZE = Integer.getInteger("asyncBatchSize", 10);
    private static final int EXECUTION_TIME_SECONDS = Integer.getInteger("executionTimeSeconds", 20);
    private static final boolean DEBUG_LOGGING_ENABLED = Boolean.getBoolean("loggingEnabled");
    private static final boolean EXTENDED_TYPES_ENABLED = Boolean.getBoolean("extendedTypesEnabled");

    private static final int BIG_DATA_TEST_NODE_COUNT = Integer.getInteger("bigDataTestNodeCount", 30_000);
    private static final int BIG_DATA_TEST_BATCH_SIZE = Integer.getInteger("bigDataTestBatchSize", 10_000);

    private static final Point POINT = point(9157, 3, 7, 12).asPoint();
    private static final LocalTime LOCAL_TIME = LocalTime.now();
    private static final LocalDateTime LOCAL_DATE_TIME = LocalDateTime.now();
    private static final LocalDate DATE = LOCAL_DATE_TIME.toLocalDate();
    private static final ZonedDateTime ZONED_DATE_TIME = ZonedDateTime.now();
    private static final Duration DURATION = Duration.ofHours(300);

    private LoggerNameTrackingLogging logging;
    private ExecutorService executor;

    InternalDriver driver;

    @BeforeEach
    void setUp() {
        logging = new LoggerNameTrackingLogging();

        driver = (InternalDriver) GraphDatabase.driver(databaseUri(), authTokenProvider(), config());

        ThreadFactory threadFactory = new DaemonThreadFactory(getClass().getSimpleName() + "-worker-");
        executor = Executors.newCachedThreadPool(threadFactory);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
        if (driver != null) {
            driver.close();
        }
    }

    @Test
    void blockingApiStressTest() throws Throwable {
        runStressTest(this::launchBlockingWorkerThreads);
    }

    @Test
    void asyncApiStressTest() throws Throwable {
        runStressTest(this::launchAsyncWorkerThreads);
    }

    @Test
    void rxApiStressTest() throws Throwable {
        assertRxIsAvailable();
        runStressTest(this::launchRxWorkerThreads);
    }

    @Test
    void blockingApiBigDataTest() {
        var bookmark = createNodesBlocking(bigDataTestBatchCount(), driver);
        readNodesBlocking(driver, bookmark);
    }

    @Test
    void asyncApiBigDataTest() throws Throwable {
        var bookmarks = createNodesAsync(bigDataTestBatchCount(), driver);
        readNodesAsync(driver, bookmarks);
    }

    @Test
    void rxApiBigDataTest() {
        assertRxIsAvailable();
        var bookmark = createNodesRx(bigDataTestBatchCount(), driver);
        readNodesRx(driver, bookmark);
    }

    private void assertRxIsAvailable() {
        assumeTrue(driver.supportsMultiDb());
    }

    private void runStressTest(Function<C, List<Future<?>>> threadLauncher) throws Throwable {
        var context = createContext();
        var resultFutures = threadLauncher.apply(context);

        var resourcesInfo = sleepAndGetResourcesInfo();
        context.stop();

        Throwable firstError = null;
        for (var future : resultFutures) {
            try {
                assertNull(future.get(10, SECONDS));
            } catch (Throwable error) {
                firstError = withSuppressed(firstError, error);
            }
        }

        printStats(context);

        if (firstError != null) {
            throw firstError;
        }

        verifyResults(context, resourcesInfo);
    }

    abstract URI databaseUri();

    abstract AuthTokenManager authTokenProvider();

    abstract Config.ConfigBuilder config(Config.ConfigBuilder builder);

    Config config() {
        @SuppressWarnings("deprecation")
        var builder = Config.builder()
                .withLogging(logging)
                .withMaxConnectionPoolSize(100)
                .withConnectionAcquisitionTimeout(1, MINUTES);
        return config(builder).build();
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

    abstract boolean handleWriteFailure(Throwable error, C context);

    abstract <A extends C> void printStats(A context);

    private List<Future<?>> launchBlockingWorkerThreads(C context) {
        var commands = createBlockingCommands();
        return IntStream.range(0, THREAD_COUNT)
                .mapToObj(i -> launchBlockingWorkerThread(executor, commands, context))
                .collect(Collectors.toList());
    }

    private List<BlockingCommand<C>> createBlockingCommands() {
        List<BlockingCommand<C>> commands = new ArrayList<>();

        commands.add(new BlockingReadQueryWithRetries<>(driver, false));
        commands.add(new BlockingReadQueryWithRetries<>(driver, true));

        commands.add(new BlockingWriteQueryWithRetries<>(this, driver, false));
        commands.add(new BlockingWriteQueryWithRetries<>(this, driver, true));

        commands.add(new BlockingWrongQueryWithRetries<>(driver));

        commands.add(new BlockingFailingQueryWithRetries<>(driver));

        commands.add(new FailedAuth<>(databaseUri(), config()));

        commands.addAll(createTestSpecificBlockingCommands());

        return commands;
    }

    private Future<Void> launchBlockingWorkerThread(
            ExecutorService executor, List<BlockingCommand<C>> commands, C context) {
        return executor.submit(() -> {
            while (context.isNotStopped()) {
                var command = randomOf(commands);
                command.execute(context);
            }
            return null;
        });
    }

    private List<Future<?>> launchRxWorkerThreads(C context) {
        var commands = createRxCommands();
        return IntStream.range(0, THREAD_COUNT)
                .mapToObj(i -> launchRxWorkerThread(executor, commands, context))
                .collect(Collectors.toList());
    }

    private List<RxCommand<C>> createRxCommands() {
        List<RxCommand<C>> commands = new ArrayList<>();

        commands.add(new RxReadQueryWithRetries<>(driver, false));
        commands.add(new RxReadQueryWithRetries<>(driver, true));

        commands.add(new RxWriteQueryWithRetries<>(this, driver, false));
        commands.add(new RxWriteQueryWithRetries<>(this, driver, true));

        commands.add(new RxFailingQueryWithRetries<>(driver));

        commands.addAll(createTestSpecificRxCommands());

        return commands;
    }

    private Future<Void> launchRxWorkerThread(ExecutorService executor, List<RxCommand<C>> commands, C context) {
        return executor.submit(() -> {
            while (context.isNotStopped()) {
                var allCommands = executeRxCommands(context, commands);
                assertNull(allCommands.get());
            }
            return null;
        });
    }

    private CompletableFuture<Void> executeRxCommands(C context, List<RxCommand<C>> commands) {
        @SuppressWarnings("unchecked")
        var executions = (CompletableFuture<Void>[])
                Array.newInstance(CompletableFuture.class, AbstractStressTestBase.ASYNC_BATCH_SIZE);
        for (var i = 0; i < AbstractStressTestBase.ASYNC_BATCH_SIZE; i++) {
            var command = randomOf(commands);
            var execution = command.execute(context);
            executions[i] = execution.toCompletableFuture();
        }
        return CompletableFuture.allOf(executions);
    }

    private List<Future<?>> launchAsyncWorkerThreads(C context) {
        var commands = createAsyncCommands();
        return IntStream.range(0, THREAD_COUNT)
                .mapToObj(i -> launchAsyncWorkerThread(executor, commands, context))
                .collect(Collectors.toList());
    }

    private List<AsyncCommand<C>> createAsyncCommands() {
        List<AsyncCommand<C>> commands = new ArrayList<>();

        commands.add(new AsyncReadQueryWithRetries<>(driver, false));
        commands.add(new AsyncReadQueryWithRetries<>(driver, true));

        commands.add(new AsyncWriteQueryWithRetries<>(this, driver, false));
        commands.add(new AsyncWriteQueryWithRetries<>(this, driver, true));

        commands.add(new AsyncWrongQueryWithRetries<>(driver));

        commands.add(new AsyncFailingQueryWithRetries<>(driver));

        commands.addAll(createTestSpecificAsyncCommands());

        return commands;
    }

    private Future<Void> launchAsyncWorkerThread(ExecutorService executor, List<AsyncCommand<C>> commands, C context) {
        return executor.submit(() -> {
            while (context.isNotStopped()) {
                var allCommands = executeAsyncCommands(context, commands);
                assertNull(allCommands.get());
            }
            return null;
        });
    }

    private CompletableFuture<Void> executeAsyncCommands(C context, List<AsyncCommand<C>> commands) {
        @SuppressWarnings("unchecked")
        var executions = (CompletableFuture<Void>[])
                Array.newInstance(CompletableFuture.class, AbstractStressTestBase.ASYNC_BATCH_SIZE);
        for (var i = 0; i < AbstractStressTestBase.ASYNC_BATCH_SIZE; i++) {
            var command = randomOf(commands);
            var execution = command.execute(context);
            executions[i] = execution.toCompletableFuture();
        }
        return CompletableFuture.allOf(executions);
    }

    private ResourcesInfo sleepAndGetResourcesInfo() throws InterruptedException {
        var halfSleepSeconds = Math.max(1, EXECUTION_TIME_SECONDS / 2);
        SECONDS.sleep(halfSleepSeconds);
        var resourcesInfo = getResourcesInfo();
        SECONDS.sleep(halfSleepSeconds);
        return resourcesInfo;
    }

    private ResourcesInfo getResourcesInfo() {
        var openFileDescriptorCount = getOpenFileDescriptorCount();
        var acquiredLoggerNames = logging.getAcquiredLoggerNames();
        return new ResourcesInfo(openFileDescriptorCount, acquiredLoggerNames);
    }

    private void verifyResults(C context, ResourcesInfo resourcesInfo) {
        assertNoFileDescriptorLeak(resourcesInfo.openFileDescriptorCount);
        assertNoLoggersLeak(resourcesInfo.acquiredLoggerNames);
        assertExpectedNumberOfNodesCreated(context.getCreatedNodesCount());
    }

    private void assertNoFileDescriptorLeak(long previousOpenFileDescriptors) {
        System.out.println("Initially open file descriptors: " + previousOpenFileDescriptors);

        // number of open file descriptors should not go up for more than 50%
        var maxOpenFileDescriptors = (long) (previousOpenFileDescriptors * 1.5);
        var currentOpenFileDescriptorCount = getOpenFileDescriptorCount();
        System.out.println("Currently open file descriptors: " + currentOpenFileDescriptorCount);

        assertThat(
                "Unexpectedly high number of open file descriptors",
                currentOpenFileDescriptorCount,
                lessThanOrEqualTo(maxOpenFileDescriptors));
    }

    private void assertNoLoggersLeak(Set<String> previousAcquiredLoggerNames) {
        var currentAcquiredLoggerNames = logging.getAcquiredLoggerNames();
        assertThat(
                "Unexpected amount of logger instances",
                currentAcquiredLoggerNames,
                equalTo(previousAcquiredLoggerNames));
    }

    private void assertExpectedNumberOfNodesCreated(long expectedCount) {
        try (var session = driver.session()) {
            var records = session.run("MATCH (n) RETURN count(n) AS nodesCount").list();
            assertEquals(1, records.size());
            var record = records.get(0);
            var actualCount = record.get("nodesCount").asLong();
            assertEquals(expectedCount, actualCount, "Unexpected number of nodes in the database");
        }
    }

    private static long getOpenFileDescriptorCount() {
        try {
            var osBean = ManagementFactory.getOperatingSystemMXBean();
            var method = osBean.getClass().getDeclaredMethod("getOpenFileDescriptorCount");
            method.setAccessible(true);
            return (long) method.invoke(osBean);
        } catch (Throwable t) {
            return 0;
        }
    }

    private static Throwable withSuppressed(Throwable firstError, Throwable newError) {
        if (firstError == null) {
            return newError;
        }
        firstError.addSuppressed(newError);
        return firstError;
    }

    private static <T> T randomOf(List<T> elements) {
        var index = ThreadLocalRandom.current().nextInt(elements.size());
        return elements.get(index);
    }

    private static int bigDataTestBatchCount() {
        if (BIG_DATA_TEST_NODE_COUNT < BIG_DATA_TEST_BATCH_SIZE) {
            return 1;
        }
        return BIG_DATA_TEST_NODE_COUNT / BIG_DATA_TEST_BATCH_SIZE;
    }

    private static Set<Bookmark> createNodesBlocking(int batchCount, Driver driver) {
        Set<Bookmark> bookmarks;

        var start = System.nanoTime();
        try (var session = driver.session()) {
            for (var i = 0; i < batchCount; i++) {
                var batchIndex = i;
                session.executeWrite(
                        tx -> createNodesInTx(tx, batchIndex, AbstractStressTestBase.BIG_DATA_TEST_BATCH_SIZE));
            }
            bookmarks = session.lastBookmarks();
        }
        var end = System.nanoTime();
        System.out.println("Node creation with blocking API took: " + NANOSECONDS.toMillis(end - start) + "ms");

        return bookmarks;
    }

    private static void readNodesBlocking(Driver driver, Set<Bookmark> bookmarks) {
        var start = System.nanoTime();
        try (var session = driver.session(builder().withBookmarks(bookmarks).build())) {
            int nodesProcessed = session.executeRead(tx -> {
                var result = tx.run("MATCH (n:Node) RETURN n");

                var nodesSeen = 0;
                while (result.hasNext()) {
                    var node = result.next().get(0).asNode();
                    nodesSeen++;

                    var labels = Iterables.asList(node.labels());
                    assertEquals(2, labels.size());
                    assertTrue(labels.contains("Test"));
                    assertTrue(labels.contains("Node"));

                    verifyNodeProperties(node);
                }
                return nodesSeen;
            });

            assertEquals(AbstractStressTestBase.BIG_DATA_TEST_NODE_COUNT, nodesProcessed);
        }
        var end = System.nanoTime();
        System.out.println("Reading nodes with blocking API took: " + NANOSECONDS.toMillis(end - start) + "ms");
    }

    private static Set<Bookmark> createNodesAsync(int batchCount, Driver driver) throws Throwable {
        var start = System.nanoTime();

        var session = driver.session(AsyncSession.class);
        CompletableFuture<Throwable> writeTransactions = completedFuture(null);

        for (var i = 0; i < batchCount; i++) {
            var batchIndex = i;
            writeTransactions = writeTransactions.thenCompose(ignore -> session.executeWriteAsync(
                    tx -> createNodesInTxAsync(tx, batchIndex, AbstractStressTestBase.BIG_DATA_TEST_BATCH_SIZE)));
        }
        writeTransactions = writeTransactions
                .exceptionally(Function.identity())
                .thenCompose(error -> safeCloseSession(session, error));

        var error = Futures.blockingGet(writeTransactions);
        if (error != null) {
            throw error;
        }

        var end = System.nanoTime();
        System.out.println("Node creation with async API took: " + NANOSECONDS.toMillis(end - start) + "ms");

        return session.lastBookmarks();
    }

    private static void readNodesAsync(Driver driver, Set<Bookmark> bookmarks) throws Throwable {
        var start = System.nanoTime();

        var session = driver.session(
                AsyncSession.class, builder().withBookmarks(bookmarks).build());
        var nodesSeen = new AtomicInteger();

        var readQuery = session.executeReadAsync(tx -> tx.runAsync("MATCH (n:Node) RETURN n")
                        .thenCompose(cursor -> cursor.forEachAsync(record -> {
                            var node = record.get(0).asNode();
                            nodesSeen.incrementAndGet();

                            var labels = Iterables.asList(node.labels());
                            assertEquals(2, labels.size());
                            assertTrue(labels.contains("Test"));
                            assertTrue(labels.contains("Node"));

                            verifyNodeProperties(node);
                        })))
                .thenApply(summary -> (Throwable) null)
                .exceptionally(Function.identity())
                .thenCompose(error -> safeCloseSession(session, error));

        var error = Futures.blockingGet(readQuery);
        if (error != null) {
            throw error;
        }

        assertEquals(AbstractStressTestBase.BIG_DATA_TEST_NODE_COUNT, nodesSeen.get());

        var end = System.nanoTime();
        System.out.println("Reading nodes with async API took: " + NANOSECONDS.toMillis(end - start) + "ms");
    }

    private Set<Bookmark> createNodesRx(int batchCount, InternalDriver driver) {
        var start = System.nanoTime();

        var session = driver.session(ReactiveSession.class);

        Flux.concat(Flux.range(0, batchCount)
                        .map(batchIndex -> session.executeWrite(tx ->
                                createNodesInTxRx(tx, batchIndex, AbstractStressTestBase.BIG_DATA_TEST_BATCH_SIZE))))
                .blockLast(); // throw any error if happened

        var end = System.nanoTime();
        System.out.println("Node creation with reactive API took: " + NANOSECONDS.toMillis(end - start) + "ms");

        return session.lastBookmarks();
    }

    private Publisher<Void> createNodesInTxRx(
            ReactiveTransactionContext tx, int batchIndex, @SuppressWarnings("SameParameterValue") int batchSize) {
        return Flux.concat(Flux.range(0, batchSize)
                .map(index -> batchIndex * batchSize + index)
                .map(nodeIndex -> {
                    var query = createNodeInTxQuery(nodeIndex);
                    return Mono.fromDirect(tx.run(query))
                            .flatMap(reactiveResult -> Mono.fromDirect(reactiveResult.consume()))
                            .then(); // As long as there is no error
                }));
    }

    private void readNodesRx(InternalDriver driver, Set<Bookmark> bookmarks) {
        var start = System.nanoTime();

        var session = driver.session(
                ReactiveSession.class, builder().withBookmarks(bookmarks).build());
        var nodesSeen = new AtomicInteger();

        var readQuery = session.executeRead(tx -> Mono.fromDirect(tx.run("MATCH (n:Node) RETURN n"))
                .flatMapMany(ReactiveResult::records)
                .doOnNext(record -> {
                    var node = record.get(0).asNode();
                    nodesSeen.incrementAndGet();

                    var labels = Iterables.asList(node.labels());
                    assertEquals(2, labels.size());
                    assertTrue(labels.contains("Test"));
                    assertTrue(labels.contains("Node"));

                    verifyNodeProperties(node);
                })
                .then());

        Flux.from(readQuery).blockLast();

        assertEquals(AbstractStressTestBase.BIG_DATA_TEST_NODE_COUNT, nodesSeen.get());

        var end = System.nanoTime();
        System.out.println("Reading nodes with async API took: " + NANOSECONDS.toMillis(end - start) + "ms");
    }

    private static Void createNodesInTx(
            TransactionContext tx, int batchIndex, @SuppressWarnings("SameParameterValue") int batchSize) {
        for (var index = 0; index < batchSize; index++) {
            var nodeIndex = batchIndex * batchSize + index;
            createNodeInTx(tx, nodeIndex);
        }
        return null;
    }

    private static void createNodeInTx(TransactionContext tx, int nodeIndex) {
        var query = createNodeInTxQuery(nodeIndex);
        tx.run(query).consume();
    }

    private static CompletionStage<Throwable> createNodesInTxAsync(
            AsyncTransactionContext tx, int batchIndex, @SuppressWarnings("SameParameterValue") int batchSize) {
        @SuppressWarnings("unchecked")
        CompletableFuture<Void>[] queryFutures = IntStream.range(0, batchSize)
                .map(index -> batchIndex * batchSize + index)
                .mapToObj(nodeIndex -> createNodeInTxAsync(tx, nodeIndex))
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(queryFutures)
                .thenApply(ignored -> (Throwable) null)
                .exceptionally(Function.identity());
    }

    private static CompletableFuture<Void> createNodeInTxAsync(AsyncTransactionContext tx, int nodeIndex) {
        var query = createNodeInTxQuery(nodeIndex);
        return tx.runAsync(query)
                .thenCompose(ResultCursor::consumeAsync)
                .thenApply(ignore -> (Void) null)
                .toCompletableFuture();
    }

    private static Query createNodeInTxQuery(int nodeIndex) {
        var query = "CREATE (n:Test:Node) SET n = $props";
        Map<String, Object> params = singletonMap("props", createNodeProperties(nodeIndex));
        return new Query(query, params);
    }

    private static Map<String, Object> createNodeProperties(int nodeIndex) {
        Map<String, Object> result = new HashMap<>();
        result.put("index", nodeIndex);
        result.put("name", "name-" + nodeIndex);
        result.put("surname", "surname-" + nodeIndex);
        result.put("long-indices", nCopies(10, (long) nodeIndex));
        result.put("double-indices", nCopies(10, (double) nodeIndex));
        result.put("booleans", nCopies(10, nodeIndex % 2 == 0));

        if (EXTENDED_TYPES_ENABLED) {
            result.put("cartPoint", POINT);
            result.put("localDateTime", LOCAL_DATE_TIME);
            result.put("zonedDateTime", ZONED_DATE_TIME);
            result.put("localTime", LOCAL_TIME);
            result.put("date", DATE);
            result.put("duration", DURATION);
        }

        return result;
    }

    private static void verifyNodeProperties(Node node) {
        var nodeIndex = node.get("index").asInt();
        assertEquals("name-" + nodeIndex, node.get("name").asString());
        assertEquals("surname-" + nodeIndex, node.get("surname").asString());
        assertEquals(nCopies(10, (long) nodeIndex), node.get("long-indices").asList());
        assertEquals(nCopies(10, (double) nodeIndex), node.get("double-indices").asList());
        assertEquals(nCopies(10, nodeIndex % 2 == 0), node.get("booleans").asList());

        if (EXTENDED_TYPES_ENABLED) {
            assertEquals(POINT, node.get("cartPoint").asPoint());
            assertEquals(LOCAL_DATE_TIME, node.get("localDateTime").asLocalDateTime());
            assertEquals(ZONED_DATE_TIME, node.get("zonedDateTime").asZonedDateTime());
            assertEquals(LOCAL_TIME, node.get("localTime").asLocalTime());
            assertEquals(DATE, node.get("date").asLocalDate());
            assertEquals(new InternalIsoDuration(DURATION), node.get("duration").asIsoDuration());
        }
    }

    private static <T> CompletionStage<T> safeCloseSession(AsyncSession session, T result) {
        return session.closeAsync().exceptionally(ignore -> null).thenApply(ignore -> result);
    }

    private record ResourcesInfo(long openFileDescriptorCount, Set<String> acquiredLoggerNames) {}

    @SuppressWarnings("deprecation")
    private static class LoggerNameTrackingLogging implements Logging {
        @Serial
        private static final long serialVersionUID = -1100018645191686024L;

        private final Logging consoleLogging = Logging.console(Level.FINE);

        @SuppressWarnings("serial")
        private final Set<String> acquiredLoggerNames = ConcurrentHashMap.newKeySet();

        @Override
        public Logger getLog(String name) {
            acquiredLoggerNames.add(name);
            if (DEBUG_LOGGING_ENABLED) {
                return consoleLogging.getLog(name);
            }
            return DevNullLogger.DEV_NULL_LOGGER;
        }

        Set<String> getAcquiredLoggerNames() {
            return new HashSet<>(acquiredLoggerNames);
        }
    }
}
