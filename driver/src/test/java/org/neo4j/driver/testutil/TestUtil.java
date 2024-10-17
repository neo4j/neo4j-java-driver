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
package org.neo4j.driver.testutil;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.SessionConfig.forDatabase;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.NoOpBookmarkManager;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.summary.CommitSummary;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.EventLoopGroupFactory;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.security.BoltSecurityPlanManager;
import org.neo4j.driver.internal.util.FixedRetryLogic;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class TestUtil {
    public static final BoltProtocolVersion DEFAULT_TEST_PROTOCOL_VERSION = BoltProtocolV4.VERSION;
    public static final BoltProtocol DEFAULT_TEST_PROTOCOL = BoltProtocol.forVersion(DEFAULT_TEST_PROTOCOL_VERSION);

    private static final long DEFAULT_WAIT_TIME_MS = MINUTES.toMillis(100);
    private static final String ALPHANUMERICS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789";
    public static final Duration TX_TIMEOUT_TEST_TIMEOUT = Duration.ofSeconds(10);

    private TestUtil() {}

    public static <T> List<T> await(Publisher<T> publisher) {
        return await(Flux.from(publisher));
    }

    public static <T> T await(Mono<T> publisher) {
        EventLoopGroupFactory.assertNotInEventLoopThread();
        return publisher.block(Duration.ofMillis(DEFAULT_WAIT_TIME_MS));
    }

    public static <T> List<T> await(Flux<T> publisher) {
        EventLoopGroupFactory.assertNotInEventLoopThread();
        return publisher.collectList().block(Duration.ofMillis(DEFAULT_WAIT_TIME_MS));
    }

    public static <T> List<T> awaitAll(List<CompletionStage<T>> stages) {
        return stages.stream().map(TestUtil::await).collect(toList());
    }

    public static <T> T await(CompletionStage<T> stage) {
        return await((Future<T>) stage.toCompletableFuture());
    }

    public static <T> T await(CompletableFuture<T> future) {
        return await((Future<T>) future);
    }

    public static <T, U extends Future<T>> T await(U future) {
        try {
            return future.get(DEFAULT_WAIT_TIME_MS, MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for future: " + future, e);
        } catch (ExecutionException e) {
            PlatformDependent.throwException(e.getCause());
            return null;
        } catch (TimeoutException e) {
            throw new AssertionError("Given future did not complete in time: " + future);
        }
    }

    public static void assertByteBufContains(ByteBuf buf, Number... values) {
        try {
            assertNotNull(buf);
            var expectedReadableBytes =
                    Arrays.stream(values).mapToInt(TestUtil::bytesCount).sum();
            assertEquals(expectedReadableBytes, buf.readableBytes(), "Unexpected number of bytes");
            for (var expectedValue : values) {
                var actualValue = read(buf, expectedValue.getClass());
                var valueType = actualValue.getClass().getSimpleName();
                assertEquals(expectedValue, actualValue, valueType + " values not equal");
            }
        } finally {
            releaseIfPossible(buf);
        }
    }

    public static void assertByteBufEquals(ByteBuf expected, ByteBuf actual) {
        try {
            assertEquals(expected, actual);
        } finally {
            releaseIfPossible(expected);
            releaseIfPossible(actual);
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> Set<T> asOrderedSet(T... elements) {
        return new LinkedHashSet<>(Arrays.asList(elements));
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> Set<T> asSet(T... elements) {
        return new HashSet<>(Arrays.asList(elements));
    }

    public static void cleanDb(Driver driver) {
        try (var session = driver.session()) {
            cleanDb(session);
        }
    }

    public static void dropDatabase(Driver driver, String database) {
        var databaseExists = databaseExists(driver, database);
        if (!databaseExists) {
            return;
        }

        try (var session = driver.session(forDatabase("system"))) {
            session.run("DROP DATABASE " + database).consume();
        }
    }

    public static void createDatabase(Driver driver, String database) {
        var databaseExists = databaseExists(driver, database);
        if (databaseExists) {
            return;
        }

        try (var session = driver.session(SessionConfig.forDatabase("system"))) {
            session.run("CREATE DATABASE " + database).consume();
        }
    }

    public static boolean databaseExists(Driver driver, String database) {
        try (var session = driver.session(forDatabase("system"))) {
            // No procedure equivalent and `call dbms.database.state("db")` also throws an exception when db doesn't
            // exist
            return session.run("SHOW DATABASES").stream()
                    .anyMatch(r -> r.get("name").asString().equals(database));
        }
    }

    public static NetworkSession newSession(BoltConnectionProvider connectionProvider, Set<Bookmark> bookmarks) {
        return newSession(connectionProvider, WRITE, bookmarks);
    }

    private static NetworkSession newSession(
            BoltConnectionProvider connectionProvider, AccessMode mode, Set<Bookmark> bookmarks) {
        return newSession(connectionProvider, mode, new FixedRetryLogic(0), bookmarks);
    }

    public static NetworkSession newSession(BoltConnectionProvider connectionProvider, AccessMode mode) {
        return newSession(connectionProvider, mode, Collections.emptySet());
    }

    public static NetworkSession newSession(BoltConnectionProvider connectionProvider, RetryLogic logic) {
        return newSession(connectionProvider, WRITE, logic, Collections.emptySet());
    }

    public static NetworkSession newSession(BoltConnectionProvider connectionProvider) {
        return newSession(connectionProvider, WRITE, Collections.emptySet());
    }

    public static NetworkSession newSession(
            BoltConnectionProvider connectionProvider,
            AccessMode mode,
            RetryLogic retryLogic,
            Set<Bookmark> bookmarks) {
        return newSession(connectionProvider, mode, retryLogic, bookmarks, true);
    }

    public static NetworkSession newSession(
            BoltConnectionProvider connectionProvider,
            AccessMode mode,
            RetryLogic retryLogic,
            Set<Bookmark> bookmarks,
            boolean telemetryDisabled) {
        return new NetworkSession(
                BoltSecurityPlanManager.insecure(),
                connectionProvider,
                retryLogic,
                defaultDatabase(),
                mode,
                bookmarks,
                null,
                -1,
                DEV_NULL_LOGGING,
                NoOpBookmarkManager.INSTANCE,
                Config.defaultConfig().notificationConfig(),
                Config.defaultConfig().notificationConfig(),
                null,
                telemetryDisabled,
                mock(AuthTokenManager.class));
    }

    public static void setupConnectionAnswers(
            BoltConnection connection, List<Consumer<ResponseHandler>> handlerConsumers) {
        given(connection.flush(any())).willAnswer(new Answer<CompletionStage<Void>>() {
            private int index;

            @Override
            public CompletionStage<Void> answer(InvocationOnMock invocation) {
                var handler = (ResponseHandler) invocation.getArguments()[0];
                var consumer = handlerConsumers.get(index++);
                consumer.accept(handler);
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    public static void verifyAutocommitRunRx(BoltConnection connection, String query) {
        then(connection)
                .should()
                .runInAutoCommitTransaction(any(), any(), any(), any(), eq(query), any(), any(), any(), any());
        then(connection).should().flush(any());
    }

    public static void verifyRunAndPull(BoltConnection connection, String query) {
        then(connection).should().run(eq(query), any());
        then(connection).should().pull(anyLong(), anyLong());
        then(connection).should(atLeastOnce()).flush(any());
    }

    public static void verifyAutocommitRunAndPull(BoltConnection connection, String query) {
        then(connection)
                .should()
                .runInAutoCommitTransaction(any(), any(), any(), any(), eq(query), any(), any(), any(), any());
        then(connection).should().pull(anyLong(), anyLong());
        then(connection).should().flush(any());
    }

    public static void verifyCommitTx(BoltConnection connection, VerificationMode mode) {
        verify(connection, mode).commit();
        verify(connection, mode).close();
    }

    public static void verifyCommitTx(BoltConnection connection) {
        verifyCommitTx(connection, times(1));
    }

    public static void verifyRollbackTx(BoltConnection connection, VerificationMode mode) {
        verify(connection, mode).rollback();
    }

    public static void verifyRollbackTx(BoltConnection connection) {
        verifyRollbackTx(connection, times(1));
        verify(connection, atLeastOnce()).close();
    }

    public static void setupFailingRun(BoltConnection connection, Throwable error) {
        given(connection.run(any(), any())).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        given(connection.pull(anyLong(), anyLong())).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var handler = (ResponseHandler) invocation.getArgument(0);
            handler.onError(error);
            handler.onComplete();
            return CompletableFuture.completedStage(null);
        });
    }

    public static void setupFailingCommit(BoltConnection connection) {
        setupFailingCommit(connection, 1);
    }

    public static void setupFailingCommit(BoltConnection connection, int times) {
        given(connection.commit()).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer(new Answer<CompletionStage<Void>>() {
            int invoked;

            @Override
            public CompletionStage<Void> answer(InvocationOnMock invocation) {
                var handler = (ResponseHandler) invocation.getArgument(0);
                if (invoked++ < times) {
                    handler.onError(new ServiceUnavailableException(""));
                } else {
                    handler.onCommitSummary(mock(CommitSummary.class));
                }
                handler.onComplete();
                return CompletableFuture.completedStage(null);
            }
        });
    }

    public static void setupFailingRollback(BoltConnection connection) {
        setupFailingRollback(connection, 1);
    }

    public static void setupFailingRollback(BoltConnection connection, int times) {
        given(connection.rollback()).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer(new Answer<CompletionStage<Void>>() {
            int invoked;

            @Override
            public CompletionStage<Void> answer(InvocationOnMock invocation) {
                var handler = (ResponseHandler) invocation.getArgument(0);
                if (invoked++ < times) {
                    handler.onError(new ServiceUnavailableException(""));
                } else {
                    handler.onCommitSummary(mock(CommitSummary.class));
                }
                handler.onComplete();
                return CompletableFuture.completedStage(null);
            }
        });
    }

    public static void setupSuccessfulRunAndPull(BoltConnection connection) {
        given(connection.run(any(), any())).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        given(connection.pull(anyLong(), anyLong())).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var handler = (ResponseHandler) invocation.getArgument(0);
            var runSummary = mock(RunSummary.class);
            given(runSummary.keys()).willReturn(Collections.emptyList());
            handler.onRunSummary(runSummary);
            var pullSummary = mock(PullSummary.class);
            given(pullSummary.metadata()).willReturn(Collections.emptyMap());
            handler.onPullSummary(pullSummary);
            handler.onComplete();
            return CompletableFuture.completedStage(null);
        });
    }

    public static void setupSuccessfulAutocommitRunAndPull(BoltConnection connection) {
        given(connection.runInAutoCommitTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willAnswer((Answer<CompletionStage<BoltConnection>>)
                        invocation -> CompletableFuture.completedStage(connection));
        given(connection.pull(anyLong(), anyLong())).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var handler = (ResponseHandler) invocation.getArgument(0);
            var runSummary = mock(RunSummary.class);
            given(runSummary.keys()).willReturn(Collections.emptyList());
            handler.onRunSummary(runSummary);
            var pullSummary = mock(PullSummary.class);
            given(pullSummary.metadata()).willReturn(Collections.emptyMap());
            handler.onPullSummary(pullSummary);
            handler.onComplete();
            return CompletableFuture.completedStage(null);
        });
    }

    public static BoltConnection connectionMock() {
        return connectionMock(new BoltProtocolVersion(4, 2));
    }

    public static BoltConnection connectionMock(BoltProtocolVersion protocolVersion) {
        var connection = mock(BoltConnection.class);
        when(connection.serverAddress()).thenReturn(BoltServerAddress.LOCAL_DEFAULT);
        when(connection.protocolVersion()).thenReturn(protocolVersion);
        return connection;
    }

    public static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static void interruptWhenInWaitingState(Thread thread) {
        CompletableFuture.runAsync(() -> {
            // spin until given thread moves to WAITING state
            do {
                sleep(500);
            } while (thread.getState() != Thread.State.WAITING);

            thread.interrupt();
        });
    }

    public static String randomString(int size) {
        var random = ThreadLocalRandom.current();
        return IntStream.range(0, size)
                .mapToObj(i -> String.valueOf(ALPHANUMERICS.charAt(random.nextInt(ALPHANUMERICS.length()))))
                .collect(Collectors.joining());
    }

    public static void assertNoCircularReferences(Throwable ex) {
        assertNoCircularReferences(ex, new ArrayList<>());
    }

    private static void assertNoCircularReferences(Throwable ex, List<Throwable> list) {
        list.add(ex);
        if (ex.getCause() != null) {
            if (list.contains(ex.getCause())) {
                throw new AssertionError("Circular reference detected", ex.getCause());
            }
            assertNoCircularReferences(ex.getCause(), list);
        }
        for (var suppressed : ex.getSuppressed()) {
            if (list.contains(suppressed)) {
                throw new AssertionError("Circular reference detected", suppressed);
            }
            assertNoCircularReferences(suppressed, list);
        }
    }

    private static void cleanDb(Session session) {
        int nodesDeleted;
        do {
            nodesDeleted = deleteBatchOfNodes(session);
        } while (nodesDeleted > 0);
    }

    private static int deleteBatchOfNodes(Session session) {
        return session.executeWrite(tx -> {
            var result = tx.run("MATCH (n) WITH n LIMIT 1000 DETACH DELETE n RETURN count(n)");
            return result.single().get(0).asInt();
        });
    }

    private static Number read(ByteBuf buf, Class<? extends Number> type) {
        if (type == Byte.class) {
            return buf.readByte();
        } else if (type == Short.class) {
            return buf.readShort();
        } else if (type == Integer.class) {
            return buf.readInt();
        } else if (type == Long.class) {
            return buf.readLong();
        } else if (type == Float.class) {
            return buf.readFloat();
        } else if (type == Double.class) {
            return buf.readDouble();
        } else {
            throw new IllegalArgumentException("Unexpected numeric type: " + type);
        }
    }

    private static int bytesCount(Number value) {
        if (value instanceof Byte) {
            return 1;
        } else if (value instanceof Short) {
            return 2;
        } else if (value instanceof Integer) {
            return 4;
        } else if (value instanceof Long) {
            return 8;
        } else if (value instanceof Float) {
            return 4;
        } else if (value instanceof Double) {
            return 8;
        } else {
            throw new IllegalArgumentException("Unexpected number: '" + value + "' or type" + value.getClass());
        }
    }

    private static void releaseIfPossible(ByteBuf buf) {
        if (buf.refCnt() > 0) {
            buf.release();
        }
    }

    public static <T extends Serializable> T serializeAndReadBack(T instance, Class<T> targetClass)
            throws IOException, ClassNotFoundException {

        var bos = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(bos)) {
            oos.writeObject(instance);
        }
        bos.close();

        try (var oos = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            return targetClass.cast(oos.readObject());
        }
    }
}
